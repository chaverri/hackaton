package com.backcountry;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SalesRowMapper implements RowMapper<UserAction> {

    private String product = "PRODUCT";

	@Override
	public UserAction mapRow(ResultSet rs, int rowNum) throws SQLException {

		UserAction userAction = new UserAction();

		userAction.setUserID(rs.getString("username"));

        String productId = rs.getString("productId"); //StringUtils.split(rs.getString("sku"), "-")[0];

        userAction.setItemId(productId);
        userAction.setTimeStamp(rs.getTimestamp("order_date"));
        userAction.setType(UserAction.ActionType.CONVERSION);
        userAction.setItemType(product);
        return userAction;
	}

}