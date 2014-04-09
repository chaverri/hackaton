package com.backcountry;


import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class WishlistRunner {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("wishlistLoaderJob")
    private Job job;

    public static AtomicInteger total = new AtomicInteger(0);
    public static HashSet<String> items = new HashSet<String>();
    public static HashSet<String> users = new HashSet<String>();

    public static long relationshipId = 0;

    public void run() {

        try {
            System.out.println("Executing job from relationshipId: " + relationshipId);


            JobParameters param = new JobParametersBuilder()
                    .addLong("relationshipId", relationshipId)
                    .addString("catalog", "bcs")
                    .toJobParameters();

            JobExecution execution = jobLauncher.run(job, param);

            System.out.println("Exit Status : " + execution.getStatus());
            System.out.println("Exit Status : " + execution.getAllFailureExceptions());

            System.out.println("Last Run relationship Id : " + relationshipId + " - users :" + users.size() + " - items:" + items.size() + " actions:" + total.get());


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
