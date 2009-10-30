require File.dirname(__FILE__) + '/database'

class SimpleJob
  cattr_accessor :runs; self.runs = 0
  def perform; @@runs += 1; end
end

class RandomRubyObject  
  def say_hello
    'hello'
  end
end

class ErrorObject

  def throw
    raise ActiveRecord::RecordNotFound, '...'
    false
  end

end

class StoryReader

  def read(story)
    "Epilog: #{story.tell}"
  end

end

class StoryReader

  def read(story)
    "Epilog: #{story.tell}"
  end

end

describe 'random ruby objects' do
  before       { Delayed::Job.delete_all }

  describe "send_later" do
    it "should respond_to :send_later method" do
      RandomRubyObject.new.should respond_to(:send_later)
    end

    it "should raise a ArgumentError if send_later is called but the target method doesn't exist" do
      lambda { RandomRubyObject.new.send_later(:method_that_deos_not_exist) }.should raise_error(NoMethodError)
    end

    it "should add a new entry to the job table when send_later is called on it" do
      Delayed::Job.count.should == 0

      RandomRubyObject.new.send_later(:to_s)

      Delayed::Job.count.should == 1
    end

    it "should add a new entry to the job table when send_later is called on the class" do
      Delayed::Job.count.should == 0

      RandomRubyObject.send_later(:to_s)

      Delayed::Job.count.should == 1
    end

    it "should run get the original method executed when the job is performed" do

      RandomRubyObject.new.send_later(:say_hello)

      Delayed::Job.count.should == 1
    end

    it "should ignore ActiveRecord::RecordNotFound errors because they are permanent" do

      ErrorObject.new.send_later(:throw)

      Delayed::Job.count.should == 1

      output = nil

      Delayed::Job.reserve do |e|
        output = e.perform
      end

      output.should == true

    end

    it "should store the object as string if its an active record" do
      story = Story.create :text => 'Once upon...'
      story.send_later(:tell)

      job =  Delayed::Job.find(:first)
      job.payload_object.class.should   == Delayed::PerformableMethod
      job.payload_object.object.should  == "AR:Story:#{story.id}"
      job.payload_object.method.should  == :tell
      job.payload_object.args.should    == []
      job.payload_object.perform.should == 'Once upon...'
    end
    
    it "should raise an exception if the object is an unsaved active record" do
      story = Story.new :text => 'Once upon...'
      lambda { story.send_later(:tell) }.should raise_error(Delayed::SerializationError)
    end

    it "should store arguments as string if they an active record" do

      story = Story.create :text => 'Once upon...'

      reader = StoryReader.new
      reader.send_later(:read, story)

      job =  Delayed::Job.find(:first)
      job.payload_object.class.should   == Delayed::PerformableMethod
      job.payload_object.method.should  == :read
      job.payload_object.args.should    == ["AR:Story:#{story.id}"]
      job.payload_object.perform.should == 'Epilog: Once upon...'
    end
  end

  describe "send_after" do
    it "should respond_to :send_after" do
      RandomRubyObject.new.should respond_to(:send_after)
    end
    
    it "should add a new entry to the job table when send_after is called on it" do
      Delayed::Job.count.should == 0

      RandomRubyObject.new.send_after(5.minutes, :to_s)

      Delayed::Job.count.should == 1
    end
    
    it "should insert a job to be run at a time in the future" do
      now = Time.now
      Time.stub!(:now => now)
      RandomRubyObject.new.send_after(5.minutes, :to_s)
      Delayed::Job.first.run_at.to_i.should == (now + 5.minutes).to_i
    end
  end

end