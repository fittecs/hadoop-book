import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;

public class JobControlExample extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    Job job1 = new Job(getConf(), "user-activity");
    Job job2 = new Job(getConf(), "charge");
    Job job3 = new Job(getConf(), "recommendation");

    ControlledJob userActivity =  new ControlledJob(job1.getConfiguration());
    ControlledJob charge =  new ControlledJob(job2.getConfiguration());
    ControlledJob recommendation =  new ControlledJob(job3.getConfiguration());
    recommendation.addDependingJob(userActivity);
    recommendation.addDependingJob(charge);

    JobControl control = new JobControl("SGR");
    control.addJob(userActivity);
    control.addJob(charge);
    control.addJob(recommendation);

    return 0;
  }
}
