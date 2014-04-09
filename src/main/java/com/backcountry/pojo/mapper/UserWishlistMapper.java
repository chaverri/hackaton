package com.backcountry.pojo.mapper;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.StringUtils;
import com.backcountry.pojo.UserWishlist;

import java.sql.ResultSet;
import java.sql.SQLException;

public class UserWishlistMapper implements RowMapper<UserWishlist> {

    private String like = "LIKE";

    @Override
    public UserWishlist mapRow(ResultSet rs, int rowNum) throws SQLException {
        UserWishlist wishlist = new UserWishlist();

        wishlist.setRelationshipId(rs.getLong("RELATIONSHIP_ID"));
        wishlist.setUser(rs.getString("user_id"));
        wishlist.setProduct(StringUtils.split(rs.getString("id"), "-")[0]);
        wishlist.setCatalog(rs.getString("SITE_CODE"));
        wishlist.setItemType(like);

        return wishlist;
    }

}