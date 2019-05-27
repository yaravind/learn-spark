# Stackoverflow Analytics with Spark

[Performance Tuning](./docs/PerfTuning)

## File Metadata

All timestamps are UTC, default format: `yyyy-MM-dd hh:mm:ss` (stored with milliseconds).

### HDFS

|File|Size|HDFS Blocks|Block size (approx.)|
|----|-------|-----|---------|
|Posts.xml|70 GB|527| 134 MB |
|Users.xml|3 GB | 25| 129 MB |
|Tags.xml|5 MB |  1|    5 MB   |
|Votes.xml|16.4 GB| 123| 133 MB|
|Comments.xml|17.7 GB| 142| 134 MB|
|PostLinks.xml|0.7 GB| 6| 116 MB|

### Posts.xml

1. Id
2. PostTypeId (listed in the PostTypes table)
    - 1 = Question
    - 2 = Answer
    - 3 = Orphaned tag wiki
    - 4 = Tag wiki excerpt
    - 5 = Tag wiki
    - 6 = Moderator nomination
    - 7 = "Wiki placeholder" (seems to only be the election description)
    - 8 = Privilege wiki
3. AcceptedAnswerId (only present if PostTypeId = 1)
4. ParentId (only present if PostTypeId = 2)
5. CreationDate
6. DeletionDate (only non-null for the SEDE PostsWithDeleted table. Deleted posts are not present on Posts. Column not present on data dump.)
7. Score
8. ViewCount (nullable)
9. Body (as rendered HTML, not Markdown)
10. OwnerUserId (only present if user has not been deleted; always -1 for tag wiki entries, i.e. the community user owns them)
11. OwnerDisplayName (nullable)
12. LastEditorUserId (nullable)
13. LastEditorDisplayName (nullable)
14. LastEditDate (e.g. 2009-03-05T22:28:34.823) - the date and time of the most recent edit to the post (nullable)
15. LastActivityDate (e.g. 2009-03-11T12:51:01.480) - datetime of the post's most recent activity
16. Title (nullable)
17. Tags (nullable, only associated with Question type)
18. AnswerCount (nullable)
19. CommentCount (nullable)
20. FavoriteCount (nullable)
21. ClosedDate (present only if the post is closed)
22. CommunityOwnedDate (present only if post is community wiki'd)

### Users.xml

1. Id (-1 is the community user. its a background process)
2. Reputation
3. CreationDate
4. DisplayName
5. LastAccessDate (Datetime user last loaded a page; updated every 30 min at most)
6. WebsiteUrl
7. Location
8. AboutMe
9. Views (Number of times the profile is viewed)
10. UpVotes (How many upvotes the user has cast)
11. DownVotes
12. ProfileImageUrl
13. EmailHash (now always blank)
14. AccountId (User's Stack Exchange Network profile ID)

### Tags.xml

1. Id
2. TagName
3. Count
4. ExcerptPostId (nullable) Id of Post that holds the excerpt text of the tag
5. WikiPostId (nullable) Id of Post that holds the wiki text of the tag

### Votes.xml

1. Id
2. PostId
3. VoteTypeId (listed in the VoteTypes table)
    - 1 = AcceptedByOriginator
    - 2 = UpMod (AKA upvote)
    - 3 = DownMod (AKA downvote)
    - 4 = Offensive
    - 5 = Favorite (UserId will also be populated)
    - 6 = Close (effective 2013-06-25: Close votes are only stored in table: PostHistory)
    - 7 = Reopen
    - 8 = BountyStart (UserId and BountyAmount will also be populated)
    - 9 = BountyClose (BountyAmount will also be populated)
    - 10 = Deletion
    - 11 = Undeletion
    - 12 = Spam
    - 15 = ModeratorReview
    - 16 = ApproveEditSuggestion
4. CreationDate Date only (2018-07-31 00:00:00 time data is purposefully removed to protect user privacy)

### PostLinks.xml

1. Id primary key
2. CreationDate when the link was created
3. PostId id of source post
4. RelatedPostId id of target/related post
5. LinkTypeId type of link
    - 1 = Linked (PostId contains a link to RelatedPostId)
    - 3 = Duplicate (PostId is a duplicate of RelatedPostId)

### Comments.xml

1. Id
2. PostId
3. Score
4. Text (Comment body)
5. CreationDate
6. UserDisplayName
7. UserId (Optional. Absent if user has been deleted)

## Derived Metrics

**Total posts = 43,872,992**

---

Group by Post Type

|           _PostType|   count|
| -------------------- | -------- |                            
|    Tag wiki excerpt|   48,593|
|              Answer|26,496,612|
|      Privilege wiki|       2|
|Moderator nomination|     312|
|            Question|17,278,709|
|    Wiki placeholder|       4|
|            Tag wiki|   48,593|
|   Orphaned tag wiki|     167|

---

Which post types have tags? 

```scala
posts
  .filter("_Tags is null")
  .groupBy("_PostType")
  .count()
  .show(100, false)
```
                                                
|_PostType           |count   |
|--------------------|--------|
|Tag wiki excerpt    |48,593   |
|Answer              |26,496,611|
|Privilege wiki      |2       |
|Moderator nomination|312     |
|Wiki placeholder    |4       |
|Tag wiki            |48,593   |
|Orphaned tag wiki   |167     |

According to the above results, only Question types have tags, except 1 Answer which has a Tag. But this is an Anamoly.
 
 Anamoly: Upon running the following query, we figured that the post with `Id:310914` associated with `_ParentId:310870` seems to be mistakenly added the Tag to the top Answer: https://stackoverflow.com/questions/310870/use-of-prototype-vs-this-in-javascript
 
 ```scala
 posts.filter("_Tags is not null AND _PostType = 'Answer'").show
 ```

---

Users who posted maximum number of questions (The OwnerUserId can be appended to this rest url to find the name: https://stackoverflow.com/users/)

```scala 
posts
    .filter($"_PostType" === "Question")
    .groupBy("_OwnerUserId")
    .count()
    .orderBy($"count".desc)
    .limit(25)
    .show
```

|_OwnerUserId| count|
| -------------------- | -------- |
|           0 (some users only have unique display name and 0 as their id. mostly the stackoverflow employees |262490|
|       39677|  2432|
|      875317|  2269|
|        4653|  1818|
|      651174|  1754|
|      117700|  1706|
|     1194415|  1666|
|      149080|  1663|
|      470184|  1663|
|     1223975|  1559|
|      179736|  1551|
|        8741|  1435|
|       65387|  1392|
|     1833945|  1357|
|     1103606|  1283|
|      859154|  1271|
|      325418|  1256|
|      784597|  1242|
|      130015|  1236|
|      785349|  1222|

---

Which Post Types doesn't contain tags (can be used as a filter criteria)

```scala
posts
  .filter("_Tags is null")
  .groupBy("_PostType")
  .count()
  .show(100, false)
```

---

Count of questions posted grouped by tags

|_Tag               |count  |
|-------------------|-------|
|javascript         |1769157|
|java               |1519528|
|c#                 |1289419|
|php                |1265498|
|android            |1176198|
|python             |1120815|
|jquery             |945632 |
|html               |806973 |
|c++                |606848 |
|ios                |591942 |
|css                |575586 |
|mysql              |551652 |
|sql                |480824 |
|asp.net            |343360 |
|ruby-on-rails      |303550 |
|c                  |297405 |
|arrays             |289019 |
|objective-c        |286871 |
|.net               |280412 |
|r                  |278145 |
|node.js            |264283 |
|angularjs          |257266 |
|json               |256180 |
|sql-server         |254342 |
|swift              |222918 |
|iphone             |219848 |
|regex              |203459 |
|ruby               |202674 |
|ajax               |196940 |
|django             |191683 |
|excel              |189241 |
|xml                |180924 |
|asp.net-mvc        |178439 |
|linux              |173470 |
|angular            |155439 |
|database           |153793 |
|spring             |147766 |
|wpf                |147661 |
|python-3.x         |146659 |
|wordpress          |146162 |
|vba                |140252 |
|string             |136890 |
|xcode              |130731 |
|windows            |127885 |
|reactjs            |125982 |
|vb.net             |122680 |
|html5              |118975 |
|eclipse            |115861 |
|multithreading     |113861 |
|mongodb            |110627 |
|laravel            |109733 |
|bash               |109013 |
|git                |108305 |
|oracle             |107164 |
|pandas             |96975  |
|postgresql         |96299  |
|twitter-bootstrap  |94418  |
|forms              |93101  |
|image              |92220  |
|macos              |90452  |
|algorithm          |89762  |
|python-2.7         |88895  |
|scala              |87125  |
|visual-studio      |85963  |
|list               |84580  |
|excel-vba          |83910  |
|winforms           |83698  |
|apache             |83453  |
|facebook           |83229  |
|matlab             |82549  |
|performance        |81532  |
|css3               |78357  |
|entity-framework   |78333  |
|hibernate          |76239  |
|typescript         |75366  |
|linq               |73206  |
|swing              |72388  |
|function           |72193  |
|amazon-web-services|71470  |
|qt                 |69661  |
|rest               |69270  |
|shell              |68965  |
|azure              |67694  |
|firebase           |66760  |
|api                |66321  |
|maven              |66226  |
|powershell         |65673  |
|.htaccess          |65043  |
|sqlite             |64987  |
|file               |62874  |
|codeigniter        |62474  |
|unit-testing       |62037  |
|perl               |61803  |
|loops              |61170  |
|symfony            |60920  |
|selenium           |60026  |
|csv                |59755  |
|google-maps        |59690  |
|uitableview        |59060  |
|web-services       |58936  |


**Total users: 10,097,978**

**Total posts.join(users) = 43,384,834 (i.e. 43,872,992 - 43,384,834 = 488,158)**

**Total Tags: 54,464**

**Total Votes: 167,002,406**

**Total Comments: **


## Reference

- [ERD Diagram](https://sedeschema.github.io/)
- [Reference](https://meta.stackexchange.com/questions/2677/database-schema-documentation-for-the-public-data-dump-and-sede)