package com.backcountry.writers;

import com.backcountry.RunScheduler;
import com.backcountry.pojo.UserWishlist;
import io.prediction.Client;
import io.prediction.UserActionItemRequestBuilder;
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

public class WishlistIOWriter implements ItemWriter<UserWishlist> {

    private String apiUrl =  "http://10.30.65.202:8000";

    private String pio_appkey = "wRMgrZaeJTEAzJSXikRaMGkIavomdw0J3b18OsCbedB09b737dcLKAVCX8TK9f5j"; //"J5icr2ZsitjuHNfgNh1fORa6chKF1KRbEcd60IVJo646IC9nULInhC7STdtgsP2u";

    TimeZone tz = TimeZone.getTimeZone("UTC");
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mmZ");

    private Client client;

    public WishlistIOWriter(){
        df.setTimeZone(tz);

        client = new Client(pio_appkey, apiUrl);
    }

    @Override
    public void write(List<? extends UserWishlist> wl) throws Exception {
        int i = 0;
        int indexToIgnoreFrom = Integer.MAX_VALUE;

        if(wl.size()== 1000){
            //Remove the tail
            Integer lastIndex = wl.size() - 1;
            String lastOrderUser = wl.get(lastIndex).getUser();

            indexToIgnoreFrom = lastIndex;
            i = lastIndex;
            String currentUser = lastOrderUser;

            do{
                currentUser = wl.get(i).getUser();
                if(currentUser.equalsIgnoreCase(lastOrderUser)){
                    indexToIgnoreFrom = i;
                }

                i--;

            }while(currentUser.equalsIgnoreCase(lastOrderUser));
        }

        i = 0;
        for(UserWishlist action : wl){
            if(i >= indexToIgnoreFrom){
                break;
            }
            System.out.println("RESTWriter - Product:"  + action.getProduct() + " - User:" + action.getUser() + " - Relationship:" + action.getRelationshipId());

            addUser(action);
            addItem(action);
            addAction(action);
//
//            //TODO: Write last date in mongo
//            //In the mean time it is going to write on a class
            RunScheduler.lastRelationshipId = action.getRelationshipId();
//
//            RunScheduler.total.incrementAndGet();
//            RunScheduler.items.add(action.getItemId());
//            RunScheduler.users.add(action.getUserID());

            i++;
        }
    }

    private Map<String, String> buildNewParameterSet(){
        Map<String, String> params = new HashMap<String, String>();
        params.put("pio_appkey", pio_appkey);

        return params;
    }

    @Async
    private void addUser(UserWishlist ua) throws InterruptedException, ExecutionException, IOException {

        client.createUser(client.getCreateUserRequestBuilder(ua.getUser()));

        System.out.println("User added:" + ua.getUser());
    }

    @Async
    private void addItem(UserWishlist ua){

        String[] itemType = new String[]{ ua.getItemType() };
        try {
            client.createItem(client.getCreateItemRequestBuilder(ua.getProduct(), itemType));
        } catch (IOException e) {
            System.out.println("Error saving this item:" + ua.getProduct() + " - " + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("Error saving this item:" + ua.getProduct() + " - " + e.getMessage());
        } catch (ExecutionException e) {
            System.out.println("Error saving this item:" + ua.getProduct() + " - " + e.getMessage());
        }

        System.out.println("Item added:" + ua.getProduct());
    }

    @Async
    private void addAction(UserWishlist ua){
        try {
            UserActionItemRequestBuilder builder = client.getUserActionItemRequestBuilder(ua.getUser(), ua.getItemType().toString().toLowerCase(), ua.getProduct());
            client.userActionItem(builder);
        } catch (IOException e) {
            System.out.println("Error saving this item:" + ua.getProduct() + " for this user:" + ua.getUser() + " - " + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("Error saving this item:" + ua.getProduct() + " - " + e.getMessage());
        } catch (ExecutionException e) {
            System.out.println("Error saving this item:" + ua.getProduct() + " - " + e.getMessage());
        }

        System.out.println("Action added:" + ua.getUser() + " - " + ua.getProduct());
    }

}
