package org.exoplatform.addons.elasticsearch.indexer;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.exoplatform.calendar.service.CalendarEvent;
import org.exoplatform.commons.api.indexing.IndexingService;
import org.exoplatform.commons.api.indexing.data.SearchEntry;
import org.exoplatform.commons.api.indexing.data.SearchEntryId;
import org.exoplatform.container.PortalContainer;
import org.exoplatform.faq.service.FAQService;
import org.exoplatform.faq.service.Question;
import org.exoplatform.forum.service.Category;
import org.exoplatform.forum.service.Forum;
import org.exoplatform.forum.service.Post;
import org.exoplatform.forum.service.Topic;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;
import org.exoplatform.services.organization.UserProfile;
import org.exoplatform.social.core.identity.model.Profile;
import org.exoplatform.social.core.space.model.Space;
import org.exoplatform.wiki.mow.api.Page;
import org.exoplatform.wiki.mow.core.api.wiki.WikiImpl;
import org.exoplatform.wiki.service.WikiService;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import javax.jcr.Node;
import java.util.*;

/**
 * Elasticsearch indexer
 */
public class HttpEsIndexer extends IndexingService {

  private Log log = ExoLogger.getLogger(HttpEsIndexer.class);

  public static final String host = (System.getProperty("elasticsearch.host") != null) ?
          System.getProperty("elasticsearch.host") : "localhost";
  public static final String port = (System.getProperty("elasticsearch.port") != null) ?
          System.getProperty("elasticsearch.port") : "9200";

  private FAQService faqService;

  public HttpEsIndexer(FAQService faqService) {
    this.faqService = faqService;
  }

  /**
   * Add entry in the indexer
   * @param searchEntry
   */
  public void add(SearchEntry searchEntry) {
    String index = searchEntry.getId().getCollection();
    String type = searchEntry.getId().getType();
    String name = searchEntry.getId().getName();
    String id = null;
    String json = null;

    // Wiki
    if ("wiki".equals(index)) {
      // FIXME A wiki page does not have an unique and constant id, need to use the JCR node UUID
      Page page = (Page) searchEntry.getContent().get("page");
      try {
        id = page.getJCRPageNode().getUUID();
      } catch (Exception e) {
        log.error("Error while retrieving the uuid for the wiki page " + type + "/" + name + " - Cause : " + e.getMessage(), e);
      }

      json = getWikiPageJSON(page);
    }
    // Calendar
    else if ("calendar".equals(index)) {
      // FIXME Only public events are triggered (what is a public event ?)
      CalendarEvent event = (CalendarEvent) searchEntry.getContent().get("event");

      id = name;

      json = getCalendarJSON(type, event);
    }
    // Answer
    else if ("answer".equals(index)) {
      Question question = (Question) searchEntry.getContent().get("question");

      id = name;

      json = getAnswerJSON(question);
    }
    // Content
    else if ("content".equals(index)) {
      Node content = (Node) searchEntry.getContent().get("content");

      id = name;

      json = getContentJSON(name, content);
    }
    // Forum
    else if ("forum".equals(index)) {
      Map<String, Object> content = searchEntry.getContent();

      id = name;

      json = getForumJSON(type, content);

    }
    // Social
    else if ("social".equals(index)) {
      if("profile".equals(type)) {
        Object profile = searchEntry.getContent().get("profile");

        id = name;

        // FIXME there are 2 types of profile: Organization UserProfile and Social UserProfile, this is very confusing. We should at least change the "index" of the Organization UserProfile to "organization" instead of "social"
        if(profile instanceof UserProfile) {
          UserProfile userProfile = (UserProfile) profile;

          json = getUserProfileJSON(userProfile);
        } else if(profile instanceof Profile) {
          Profile userSocialProfile = (Profile) profile;

          json = getSocialProfileJSON(userSocialProfile);
        }
      } else if("space".equals(type)) {
        Space space = (Space) searchEntry.getContent().get("space");

        id = name;

        json = getSpaceJSON(space);
      }
    }
    else {
      log.debug("The indexer for " + index + " is not implemented yet!");
    }

    try {
      HttpClient client = new DefaultHttpClient();
      HttpPost request = new HttpPost("http://" + host + ":" + port + "/" + index + "/" + type + "/" + id);
      request.setEntity(new StringEntity(json, "UTF-8"));
      HttpResponse response = client.execute(request);
    } catch (Exception e) {
      log.error("Error while indexing in Elasticsearch - Cause : " + e.getMessage(), e);
    }
  }

  /**
   * Update entry in the indexer
   * @param searchEntryId
   * @param changes
   */
  public void update(SearchEntryId searchEntryId, Map<String, Object> changes) {

    String index = searchEntryId.getCollection();
    String type = searchEntryId.getType();
    String name = searchEntryId.getName();
    String id = null;
    String json = null;

    if ("wiki".equals(index)) {
      Page page = (Page) changes.get("page");
      try {
        id = page.getJCRPageNode().getUUID();
      } catch (Exception e) {
        log.error("Error while retrieving the uuid for the wiki page " + type + "/" + name + " - Cause : " + e.getMessage(), e);
      }

      json = getWikiPageJSON(page);
    }
    // Calendar
    else if ("calendar".equals(index)) {
      CalendarEvent event = (CalendarEvent) changes.get("event");

      id = name;

      json = getCalendarJSON(type, event);
    }
    // Answer
    else if ("answer".equals(index)) {
      Question question = (Question) changes.get("question");

      id = name;

      json = getAnswerJSON(question);
    }
    // Content
    else if ("content".equals(index)) {
      Node content = (Node) changes.get("content");

      id = name;

      json = getContentJSON(name, content);
    }
    // Forum
    else if ("forum".equals(index)) {
      id = name;

      json = getForumJSON(type, changes);
    }
    // Social
    else if ("social".equals(index)) {

      if("profile".equals(type)) {
        Object profile = changes.get("profile");

        id = name;

        if(profile instanceof UserProfile) {
          UserProfile userProfile = (UserProfile) profile;

          json = getUserProfileJSON(userProfile);
        } else if(profile instanceof Profile) {
          Profile userSocialProfile = (Profile) profile;

          json = getSocialProfileJSON(userSocialProfile);
        }
      } else if("space".equals(type)) {
        Space space = (Space) changes.get("space");

        id = name;

        json = getSpaceJSON(space);
      }
    }
    else {
      log.debug("The indexer for " + index + " is not implemented yet!");
    }

    try {
      HttpClient client = new DefaultHttpClient();
      HttpPost request = new HttpPost("http://" + host + ":" + port + "/" + index + "/" + type + "/" + id);
      request.setEntity(new StringEntity(json, "UTF-8"));
      client.execute(request);
    } catch (Exception e) {
      log.error("Error while indexing in Elasticsearch - Cause : " + e.getMessage(), e);
    }
  }

  /**
   * Delete entry in the indexer
   * @param searchEntryId
   */
  public void delete(SearchEntryId searchEntryId) {

    String index = searchEntryId.getCollection();
    String type = searchEntryId.getType();
    String name = searchEntryId.getName();
    String id = null;

    if ("wiki".equals(index)) {
      // FIXME A wiki page does not have an unique id, it is a composed id : wiki type, wiki owner, wiki page name. SearchEntryId does not have enough attributes to handle such a case.
      // FIXME Because of the changing id of the wiki pages, the only stable id is the JCR node uuid. So we need to retrieve the page in the trash to get that uuid.
      try {
        WikiService wikiService = (WikiService) PortalContainer.getInstance().getComponentInstanceOfType(WikiService.class);
        Page page = ((WikiImpl) wikiService.getWiki(type, "intranet")).getTrash().getPage(name); // temporarily hard-code wiki owner
        id = page.getJCRPageNode().getUUID();
      } catch (Exception e) {
        log.error("Error while retrieving the uuid for the wiki page " + type + "/" + name + " - Cause : " + e.getMessage(), e);
      }

    }
    // Calendar
    else if ("calendar".equals(index)) {
      id = name;
    }
    // Answer
    else if ("answer".equals(index)) {
      // FIXME Answers listener provides the question id for the creation and update, and provides the social activity id for the deletion (mix of data and responsability between answers and social), so we need to retrieve the question id from the activity id
      try {
        List<Question> allQuestions = faqService.getAllQuestions().getAll();
        for(Question question : allQuestions) {
          String activityId = faqService.getActivityIdForQuestion(question.getId());
          if(name.equals(activityId)) {
            id = question.getId();
          }
        }
      } catch (Exception e) {
        log.error("Error while retrieving the question matching the activity id " + name, e);
      }
    }
    // Forum
    else if ("forum".equals(index)) {
      id = name;
    }
    // Social
    else if ("social".equals(index)) {
      id = name;
    }
    else {
      log.debug("The indexer for " + index + " is not implemented yet!");
    }

    try {
      HttpClient client = new DefaultHttpClient();
      HttpDelete request = new HttpDelete("http://" + host + ":" + port + "/" + index + "/" + type + "/" + id);
      client.execute(request);
    } catch (Exception e) {
      log.error("Error while indexing in Elasticsearch - Cause : " + e.getMessage(), e);
    }
  }

  private String getSocialProfileJSON(Profile userSocialProfile) {
    String json;JSONObject obj = new JSONObject();
    obj.put("username", userSocialProfile.getId());
    Map<String, Object> userInfoMap = userSocialProfile.getProperties();
    for(String attributeName: userInfoMap.keySet()) {
      obj.put(attributeName, userSocialProfile.getProperty(attributeName));
    }
    json = obj.toJSONString();
    return json;
  }

  private String getForumJSON(String type, Map<String, Object> content) {
    String json;JSONObject obj = new JSONObject();
    if(type.equals("category")) {
      Category category = (Category) content.get("category");
      obj.put("title", category.getCategoryName());
      obj.put("description", category.getDescription());
      obj.put("createdDate", category.getCreatedDate() != null ? category.getCreatedDate().getTime() : null);
      obj.put("updatedDate", category.getModifiedDate() != null ? category.getModifiedDate().getTime() : null);
    } else if(type.equals("forum")) {
      Forum forum = (Forum) content.get("forum");
      obj.put("title", forum.getForumName());
      obj.put("description", forum.getDescription());
      obj.put("createdDate", forum.getCreatedDate() != null ? forum.getCreatedDate().getTime() : null);
      obj.put("updatedDate", forum.getModifiedDate() != null ? forum.getModifiedDate().getTime() : null);
      obj.put("category", forum.getCategoryId());
    } else if(type.equals("topic")) {
      Topic topic = (Topic) content.get("topic");
      obj.put("title", topic.getTopicName());
      obj.put("description", topic.getDescription());
      obj.put("createdDate", topic.getCreatedDate() != null ? topic.getCreatedDate().getTime() : null);
      obj.put("updatedDate", topic.getModifiedDate() != null ? topic.getModifiedDate().getTime() : null);
      obj.put("owner", topic.getOwner());
      obj.put("url", topic.getLink());
      obj.put("category", topic.getCategoryId());
      obj.put("forum", topic.getForumId());
    } else if(type.equals("post")) {
      Post post = (Post) content.get("post");
      obj.put("title", post.getName());
      obj.put("description", post.getMessage());
      obj.put("createdDate", post.getCreatedDate() != null ? post.getCreatedDate().getTime() : null);
      obj.put("updatedDate", post.getModifiedDate() != null ? post.getModifiedDate().getTime() : null);
      obj.put("owner", post.getOwner());
      obj.put("url", post.getLink());
      obj.put("category", post.getCategoryId());
      obj.put("forum", post.getForumId());
      obj.put("topic", post.getTopicId());
    }
    json = obj.toJSONString();
    return json;
  }

  private String getContentJSON(String name, Node content) {
    String json;JSONObject obj = new JSONObject();
    Map<String, String> attributesMapping = new HashMap<String, String>();
    attributesMapping.put("exo:title", "title");
    attributesMapping.put("exo:dateCreated", "dateCreated");
    attributesMapping.put("exo:dateModified", "dateModified");
    attributesMapping.put("exo:owner", "owner");
    attributesMapping.put("jcr:primaryType", "contentType");
    attributesMapping.put("publication:currentState", "publicationState");

    for(String attribute: attributesMapping.keySet()) {
      String attributeValue = "";
      try {
        if(content.hasProperty(attribute)) {
          attributeValue = content.getProperty(attribute).getString();
        }
      } catch (Exception e) {
        log.error("Error while retrieving " + attribute + " property of the content " + name, e);
      }
      obj.put(attributesMapping.get(attribute), attributeValue);
    }

    json = obj.toJSONString();
    return json;
  }

  private String getCalendarJSON(String type, CalendarEvent event) {
    String json;JSONObject obj = new JSONObject();
    obj.put("type", type);
    obj.put("message", event.getMessage());
    obj.put("description", event.getDescription());
    json = obj.toJSONString();
    return json;
  }


  private String getSpaceJSON(Space space) {
    String json;JSONObject obj = new JSONObject();
    obj.put("displayName", space.getDisplayName());
    obj.put("description", space.getDescription());
    obj.put("createdTime", space.getCreatedTime());
    obj.put("url", space.getUrl());
    obj.put("avatarUrl", space.getAvatarUrl());
    obj.put("editor", space.getEditor());
    obj.put("group", space.getGroupId());
    JSONArray managers = new JSONArray();
    managers.addAll(space.getManagers() != null ? Arrays.asList(space.getManagers()) : Collections.EMPTY_LIST);
    obj.put("managers", managers);
    JSONArray members = new JSONArray();
    members.addAll(space.getMembers() != null ? Arrays.asList(space.getMembers()) : Collections.EMPTY_LIST);
    obj.put("members", members);
    JSONArray invitedUsers = new JSONArray();
    invitedUsers.addAll(space.getInvitedUsers() != null ? Arrays.asList(space.getInvitedUsers()) : Collections.EMPTY_LIST);
    obj.put("invitedUsers", invitedUsers);
    JSONArray pendingUsers = new JSONArray();
    pendingUsers.addAll(space.getPendingUsers() != null ? Arrays.asList(space.getPendingUsers()) : Collections.EMPTY_LIST);
    obj.put("pendingUsers", pendingUsers);
    obj.put("registration", space.getRegistration());
    obj.put("visibility", space.getVisibility());
    json = obj.toJSONString();
    return json;
  }

  private String getUserProfileJSON(UserProfile userProfile) {
    String json;JSONObject obj = new JSONObject();
    obj.put("username", userProfile.getUserName());
    Map<String, String> userInfoMap = userProfile.getUserInfoMap();
    for(String attributeName: userInfoMap.keySet()) {
      obj.put(attributeName, userProfile.getAttribute(attributeName));
    }
    json = obj.toJSONString();
    return json;
  }

  private String getAnswerJSON(Question question) {
    String json;JSONObject obj = new JSONObject();
    obj.put("title", question.getQuestion());
    obj.put("detail", question.getDetail());
    obj.put("author", question.getAuthor());
    obj.put("createdDate", question.getCreatedDate().getTime());
    obj.put("updatedDate", question.getTimeOfLastActivity());
    obj.put("category", question.getCategoryId());
    obj.put("url", question.getLink());
    json = obj.toJSONString();
    return json;
  }

  private String getWikiPageJSON(Page page) {
    String json;JSONObject obj = new JSONObject();
    obj.put("title", page.getTitle());
    obj.put("owner", page.getOwner());
    obj.put("author", page.getAuthor());
    obj.put("url", page.getURL());
    obj.put("createdDate", page.getCreatedDate().getTime());
    obj.put("updatedDate", page.getUpdatedDate().getTime());
    obj.put("body", page.getContent().getText());
    json = obj.toJSONString();
    return json;
  }
}
