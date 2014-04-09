package com.backcountry;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.scheduling.annotation.EnableAsync;

import java.sql.Connection;
import java.sql.DriverManager;

@EnableAsync
public class App {

	public static void main(String[] args) {

		App obj = new App();
		obj.run();

	}

	private void run() {

        //TODO: remove
        try {
            Class.forName("org.postgresql.Driver");
            Connection connection = null;
            connection = DriverManager.getConnection(
                "jdbc:oracle:thin:@10.42.35.4:1521/ATGPRD4.pp.bcinfra.net", "atg_wishlist", "atgw15hl1st$");
            connection.close();
            System.out.println("Connection OK");
        } catch(Exception e){
            e.printStackTrace();
        }



		String[] springConfig = { "spring/batch/jobs/job-extract-users.xml" };

		ApplicationContext context = new ClassPathXmlApplicationContext(springConfig);

        System.out.println("Loader started ...");

        /*
		JobLauncher jobLauncher = (JobLauncher) context.getBean("jobLauncher");
		Job job = (Job) context.getBean("testJob");

		try {

			JobParameters param = new JobParametersBuilder()
                                        .addDate("date", new GregorianCalendar(2014, 1, 1).getTime())
                                        .addString("catalog", "bcs")
                                        .toJobParameters();
			//JobParameters param = new JobParametersBuilder().addString("name", "user_c").toJobParameters();
			
			JobExecution execution = jobLauncher.run(job, param);
			System.out.println("Exit Status : " + execution.getStatus());
			System.out.println("Exit Status : " + execution.getAllFailureExceptions());

		} catch (Exception e) {
			e.printStackTrace();

		}

		System.out.println("Done");
       */
	}

}
