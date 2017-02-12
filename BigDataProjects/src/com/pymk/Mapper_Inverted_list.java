
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.logging.Logger;

public class WhotoFollow {

    /**
     * *****************
     */
    /**
     * Mapper      *
     */
    /**
     * *****************
     */
   public static class Mapper1 extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
        	Logger log = Logger.getLogger(Mapper1.class.getName());
            StringTokenizer st = new StringTokenizer(values.toString());
            log.info("the values in string are " + st.toString());
            IntWritable user = new IntWritable(Integer.parseInt(st.nextToken()));
            
            log.info("the values in user are " + user);
            // 'friends' will store the list of friends of user 'user'
            ArrayList<Integer> followers = new ArrayList<>();
            // First, go through the list of all friends of user 'user' and emit 
            // (user,-friend)
            // 'friend1' will be used in the emitted pair
            IntWritable friend1 = new IntWritable();
            while (st.hasMoreTokens()) {
               // Integer friend = Integer.parseInt(st.nextToken());
                //friend1.set(-user);
                IntWritable friend = new IntWritable(Integer.parseInt(st.nextToken()));
                context.write(friend,user); 
                log.info("the values in postive frinds new are " + friend +","+ user);
                //friend1.set(-friend.get());
                log.info("the values in friend new are " + friend1.toString());
               context.write(friend1,user); ////replaced user to friend2
               //log.info("the value of friend is" + friend
                
               log.info("the values in context friend1 " + friend1 +","+ user);
               
               //log.info(" the pair is " ,context.write(friend, friend1));
               // context.write(friend1, user);
              
               followers.add(friend.get());
                // save the friends of user 'user' for later
            }
            
            log.info("the values in followees are " + followers.toString());
            // Now we can emit all (a,b) and (b,a) pairs
            // where a!=b and a & b are friends of user 'user'.
            // We use the same algorithm as before.
           ArrayList<Integer> seenFriends = new ArrayList<>();
            //log.info("the values in seen friends are " + seenFriends.toString());
            // The element in the pairs that will be emitted.
            IntWritable friend2 = new IntWritable();
            for (Integer friend : followers) {
                friend1.set(friend);
                log.info("the values in seen friend1 now are " + friend1.toString());
               
                for (Integer seenFriend : seenFriends) {
                    friend2.set(seenFriend);                
                    context.write(friend1, friend2);
                    log.info("the values in context seenfriend1 " + friend1 +","+ friend2);
                    
                    context.write(friend2, friend1);
                    log.info("the values in context seenfriend2 " + friend1 +","+ friend2);
                }
                seenFriends.add(friend1.get());
                log.info("the values in seen seenFriends now are " + seenFriends.toString());
            }
        }
    }


   
      

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Who to follow");
        job.setJarByClass(WhotoFollow.class);
        job.setMapperClass(Mapper1.class);
       // job.setMapperClass(InverseMapper.class);
      //  job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
