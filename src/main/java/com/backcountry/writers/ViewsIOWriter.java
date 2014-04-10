package com.backcountry.writers;

import com.backcountry.UserAction;
import com.backcountry.ViewsRunner;
import io.prediction.Client;
import io.prediction.UserActionItemRequestBuilder;
import org.joda.time.DateTime;
import org.springframework.batch.item.ItemWriter;
import org.springframework.scheduling.annotation.Async;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

/**
 * Created with IntelliJ IDEA.
 * User: dachaverri
 * Date: 4/9/14
 * Time: 5:34 PM
 * To change this template use File | Settings | File Templates.
 */
public class ViewsIOWriter implements ItemWriter<UserAction> {

    private String apiUrl =  "http://localhost:8000";

    private String pio_appkey = "wRMgrZaeJTEAzJSXikRaMGkIavomdw0J3b18OsCbedB09b737dcLKAVCX8TK9f5j"; //"J5icr2ZsitjuHNfgNh1fORa6chKF1KRbEcd60IVJo646IC9nULInhC7STdtgsP2u";

    TimeZone tz = TimeZone.getTimeZone("UTC");
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mmZ");

    private Client client;

    public ViewsIOWriter(){
        df.setTimeZone(tz);

        client = new Client(pio_appkey, apiUrl);
    }

    @Override
    public void write(List<? extends UserAction> ts) throws Exception {

        for(UserAction action : ts){

            //System.out.println("RESTWriter - Product:"  + action.getItemId() + " - User:" + action.getUserID() + " - Time:" + df.format(action.getTimeStamp()));

            addUser(action);
            addItem(action);
            addAction(action);

            //TODO: Write last date in mongo
            //In the mean time it is going to write on a class
            ViewsRunner.lastRunDate = action.getTimeStamp();

            ViewsRunner.total.incrementAndGet();
            ViewsRunner.items.add(action.getItemId());
            ViewsRunner.users.add(action.getUserID());

        }
    }

    private Map<String, String> buildNewParameterSet(){
        Map<String, String> params = new HashMap<String, String>();
        params.put("pio_appkey", pio_appkey);

        return params;
    }

    @Async
    private void addUser(UserAction ua) throws InterruptedException, ExecutionException, IOException {

        client.createUser(client.getCreateUserRequestBuilder(ua.getUserID()));

        System.out.println("User added:" + ua.getUserID());
    }

    @Async
    private void addItem(UserAction ua){

        String[] itemType = new String[]{ ua.getItemType() };
        try {
            client.createItem(client.getCreateItemRequestBuilder(ua.getItemId(), itemType));
        } catch (IOException e) {
            System.out.println("Error saving this item:" + ua.getItemId() + " - " + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("Error saving this item:" + ua.getItemId() + " - " + e.getMessage());
        } catch (ExecutionException e) {
            System.out.println("Error saving this item:" + ua.getItemId() + " - " + e.getMessage());
        }

        System.out.println("Item added:" + ua.getItemId());
    }

    @Async
    private void addAction(UserAction ua){

        try {
            UserActionItemRequestBuilder builder = client.getUserActionItemRequestBuilder(ua.getUserID(), ua.getType().toString().toLowerCase(), ua.getItemId()).t(new DateTime(ua.getTimeStamp()));
            client.userActionItem(builder);
        } catch (IOException e) {
            System.out.println("Error saving this item:" + ua.getItemId() + " for this user:" + ua.getUserID() + " - " + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("Error saving this item:" + ua.getItemId() + " - " + e.getMessage());
        } catch (ExecutionException e) {
            System.out.println("Error saving this item:" + ua.getItemId() + " - " + e.getMessage());
        }

        System.out.println("Action added:" + ua.getUserID() + " - " + ua.getItemId());
    }
}
