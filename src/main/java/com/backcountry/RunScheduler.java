package com.backcountry;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class RunScheduler {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;

    public static AtomicInteger total = new AtomicInteger(0);
    public static HashSet<String> items = new HashSet<String>();
    public static HashSet<String> users = new HashSet<String>();

    public static Date lastRunDate = new GregorianCalendar(2014, 0, 1).getTime();

    public void run() {

        try {
            Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String date = formatter.format(lastRunDate);

            System.out.println("Executing job from date: " + date);


            JobParameters param = new JobParametersBuilder()
                    .addString("date", date)
                    .addString("catalog", "bcs")
                    .toJobParameters();

            JobExecution execution = jobLauncher.run(job, param);

            System.out.println("Exit Status : " + execution.getStatus());
            System.out.println("Exit Status : " + execution.getAllFailureExceptions());

            date = formatter.format(lastRunDate);
            System.out.println("Last Run Date : " + date + " - users :" + users.size() + " - items:" + items.size() + " actions:" + total.get());


        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
