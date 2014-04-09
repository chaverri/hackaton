package com.backcountry;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class ViewsRowMapper implements RowMapper<UserAction> {

    private String product = "PRODUCT";
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");

    @Override
    public UserAction mapRow(ResultSet rs, int rowNum) throws SQLException {

        UserAction userAction = new UserAction();

        userAction.setUserID(rs.getString("USER_ID"));

        String productId = rs.getString("STYLE_ID"); //StringUtils.split(rs.getString("sku"), "-")[0];

        userAction.setItemId(productId);

        java.util.Date timeStamp = null;

        try {
            timeStamp = formatter.parse(rs.getString("INV_EVENT_DATE_ID"));
        } catch (ParseException e) {
            System.out.println(e.getMessage());
        }

        userAction.setTimeStamp(timeStamp);
        userAction.setType(UserAction.ActionType.VIEW);
        userAction.setItemType(product);
        return userAction;
    }

}