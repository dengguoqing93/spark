package org.spiral.chap5

import java.sql.Timestamp

/**
  * 数据按行解析
  *
  * @author dengguoqing
  * @date 2020/1/3
  * @since 1.0 Version
  * @copyright spiral
  */
case class Post(commentCount: Option[Int], lastActivityDate: Option[Timestamp],
                ownerUserId: Option[Long], body: String, score: Option[Int],
                creationDate: Option[Timestamp], viewCount: Option[Int], title: String,
                tags: String, answerCount: Option[Int], acceptedAnswerId: Option[Long],
                postTypeId: Option[Long], id: Long)
