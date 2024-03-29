﻿using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Linq;
using ExpressBase.Common.ServerEvents_Artifacts;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Data;
using ServiceStack;
using System.Text;
using System.Data.Common;
using ExpressBase.Common;
using Newtonsoft.Json;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Helpers;
using ExpressBase.Security;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
    public class NotificationService : EbBaseService
    {
        public NotificationService(IEbConnectionFactory _dbf, IEbServerEventClient _sec) : base(_dbf, _sec) { }

        public NotifyLogOutResponse Post(NotifyLogOutRequest request)
        {
            NotifyLogOutResponse res = new NotifyLogOutResponse();
            this.ServerEventClient.Post<NotifyResponse>(new NotifySubscriptionRequest
            {
                Msg = "LogOut",
                Selector = "cmd.onLogOut"
            });
            return res;
        }

        public NotifyByUserIDResponse Post(NotifyByUserIDRequest request)
        {
            NotifyByUserIDResponse res = new NotifyByUserIDResponse();
            Notifications n = new Notifications();
            List<NotificationInfo> Notification = new List<NotificationInfo>();
            try
            {
                if (request.Link != null && request.Title != null)
                {
                    string notification_id = GenerateNotificationId();
                    Notification.Add(new NotificationInfo
                    {
                        Link = request.Link,
                        Title = request.Title,
                        NotificationId = notification_id,
                        Duration = "Today"
                    });
                    n.Notification = Notification;
                    string str = string.Format(@"select email from eb_users where id = {0} ", request.UsersID);
                    EbDataTable dt = EbConnectionFactory.DataDB.DoQuery(str);
                    if (dt.Rows.Count > 0)
                    {
                        string user_auth_id = request.SolnId + ":" + dt.Rows[0][0].ToString() + ":uc";
                        if (this.ServerEventClient.BearerToken == null || this.ServerEventClient.RefreshToken == null)
                        {
                            this.ServerEventClient.BearerToken = request.BToken;
                            this.ServerEventClient.RefreshToken = request.RToken;
                        }
                        this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
                        {
                            Msg = JsonConvert.SerializeObject(n),
                            Selector = "cmd.onNotification",
                            ToUserAuthId = request.User_AuthId,
                            NotificationId = notification_id,
                            NotifyUserId = request.UsersID
                        });
                    }

                }
                else
                {
                    throw new Exception("Notification Title or Link Empty");
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            return res;
        }

        public NotifyByUserIDsResponse Post(NotifyByUserIDsRequest request)
        {
            string msg;
            try
            {
                if (string.IsNullOrWhiteSpace(request.Link))
                    throw new Exception("Link is empty");
                if (string.IsNullOrWhiteSpace(request.Title))
                    throw new Exception("Title is empty");
                if (string.IsNullOrWhiteSpace(request.UserIDs))
                    throw new Exception("User Id(s) is empty");

                string[] arr = request.UserIDs.Split(',');
                IEnumerable<int> iarr = arr.Select(e => { return int.TryParse(e, out int t) ? t : 0; }).Where(e => e > 0);
                if (arr.Length == 0 || arr.Length != iarr.Count())
                    throw new Exception("Bad request");


                if (this.ServerEventClient.BearerToken == null || this.ServerEventClient.RefreshToken == null)
                {
                    this.ServerEventClient.BearerToken = request.BToken;
                    this.ServerEventClient.RefreshToken = request.RToken;
                }

                foreach (string uid in arr)
                {
                    string notification_id = GenerateNotificationId();
                    Notifications n = new Notifications()
                    {
                        Notification = new List<NotificationInfo>()
                        {
                            new NotificationInfo
                            {
                                Link = request.Link,
                                Title = request.Title,
                                NotificationId = notification_id,
                                Duration = "Today"
                            }
                        }
                    };
                    this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
                    {
                        Msg = JsonConvert.SerializeObject(n),
                        Selector = "cmd.onNotification",
                        ToUserAuthId = request.SolnId + ":" + uid + ":uc",
                        NotificationId = notification_id,
                        NotifyUserId = request.UserId
                    });
                }
                msg = "success";
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception in NotifyByUserIDsRequest. Message: {e.Message}\n Stack Trace: {e.StackTrace}");
                msg = e.Message;
            }
            return new NotifyByUserIDsResponse() { Message = msg };
        }

        public NotifyByUserRoleResponse Post(NotifyByUserRoleRequest request)
        {
            NotifyByUserRoleResponse res = new NotifyByUserRoleResponse();
            Notifications n = new Notifications();
            List<NotificationInfo> Notification = new List<NotificationInfo>();
            Dictionary<int, string> user_details = new Dictionary<int, string>();
            try
            {
                if (request.Link != null && request.Title != null)
                {
                    string notification_id = GenerateNotificationId();
                    Notification.Add(new NotificationInfo
                    {
                        Link = request.Link,
                        Title = request.Title,
                        NotificationId = notification_id,
                        Duration = "Today"

                    });
                    n.Notification = Notification;
                    foreach (int role_id in request.RoleID)
                    {
                        string str = string.Format(@"select ru.user_id, u.email from eb_role2user as ru, eb_users as u where ru.role_id = '{0}' and ru.user_id = u.id ", role_id);
                        EbDataTable dt = EbConnectionFactory.DataDB.DoQuery(str);
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            string user_auth_id = request.SolutionId + ":" + dt.Rows[i][1].ToString() + ":uc";
                            if (!user_details.ContainsKey(int.Parse(dt.Rows[i][0].ToString())))
                                user_details.Add(int.Parse(dt.Rows[i][0].ToString()), user_auth_id);
                        }
                    }

                    this.ServerEventClient.Post<NotifyResponse>(new NotifyUsersRequest
                    {
                        Msg = JsonConvert.SerializeObject(n),
                        Selector = "cmd.onNotification",
                        NotificationId = notification_id,
                        UsersDetails = user_details,
                        SolnId = request.SolutionId
                    });
                }
                else
                {
                    throw new Exception("Notification Title or Link Empty");
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            return res;
        }

        public NotifyByUserGroupResponse Post(NotifyByUserGroupRequest request)
        {
            NotifyByUserGroupResponse res = new NotifyByUserGroupResponse();
            Notifications n = new Notifications();
            List<NotificationInfo> Notification = new List<NotificationInfo>();
            Dictionary<int, string> user_details = new Dictionary<int, string>();
            try
            {
                if (request.Link != null && request.Title != null)
                {
                    string notification_id = GenerateNotificationId();
                    Notification.Add(new NotificationInfo
                    {
                        Link = request.Link,
                        Title = request.Title,
                        NotificationId = notification_id,
                        Duration = "Today"

                    });
                    n.Notification = Notification;
                    foreach (int grp_id in request.GroupId)
                    {
                        string str = string.Format(@" select ug.userid, u.email from eb_user2usergroup as ug, eb_users as u where ug.groupid ='{0}' and ug.userid = u.id  ", grp_id);
                        EbDataTable dt = EbConnectionFactory.DataDB.DoQuery(str);
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            string user_auth_id = request.SolutionId + ":" + dt.Rows[i][1].ToString() + ":uc";
                            if (!user_details.ContainsKey(int.Parse(dt.Rows[i][0].ToString())))
                                user_details.Add(int.Parse(dt.Rows[i][0].ToString()), user_auth_id);
                        }
                    }
                    this.ServerEventClient.Post<NotifyResponse>(new NotifyUsersRequest
                    {
                        Msg = JsonConvert.SerializeObject(n),
                        Selector = "cmd.onNotification",
                        ToUserAuthId = request.UserAuthId,
                        NotificationId = notification_id,
                        UsersDetails = user_details
                    });
                }
                else
                {
                    throw new Exception("Notification Title or Link Empty");
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            return res;
        }

        public GetNotificationFromDbResponse Post(GetNotificationFromDbRequest request)
        {
            GetNotificationFromDbResponse res = new GetNotificationFromDbResponse();
            List<NotificationInfo> n = new List<NotificationInfo>();
            this.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);

            string str = string.Format(@"UPDATE eb_notifications SET message_seen = 'T' WHERE notification_id = '{0}' AND user_id = '{1}';",
                    request.NotificationId, request.UserId);
            this.EbConnectionFactory.DataDB.DoNonQuery(str, new DbParameter[] { });

            string str1 = string.Format(@"
                                                SELECT notification_id, notification, created_at 
                                                FROM eb_notifications 
                                                WHERE user_id = '{0}'
                                                AND message_seen ='F'", request.UserId);

            EbDataTable dt = EbConnectionFactory.DataDB.DoQuery(str1);

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string notif = dt.Rows[i]["notification"].ToString();
                Notifications list = JsonConvert.DeserializeObject<Notifications>(notif);
                DateTime created_dtime = Convert.ToDateTime(dt.Rows[i]["created_at"].ToString());
                string duration = created_dtime.TimeAgo();
                n.Add(new NotificationInfo
                {
                    Link = list.Notification[0].Link,
                    NotificationId = list.Notification[0].NotificationId,
                    Title = list.Notification[0].Title,
                    Duration = duration
                });
            }
            res.Notifications = n;
            return res;
        }

        string GenerateNotificationId()
        {
            string notify_id = string.Empty;
            Random rnd = new Random();
            string[] str = new string[10];
            for (int i = 0; i < str.Length; i++)
            {
                str[i] = GenerateStr(rnd);
            }
            StringBuilder builder = new StringBuilder();
            foreach (string value in str)
            {
                builder.Append(value);
            }
            builder.Append(DateTime.Now.ToString("yyyyMMddHHmmssffff"));
            notify_id = builder.ToString();
            return notify_id;
        }

        public static string GenerateStr(Random random)
        {
            string characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
            StringBuilder result = new StringBuilder(2);
            for (int i = 0; i < 2; i++)
            {
                result.Append(characters[random.Next(characters.Length)]);
            }
            return result.ToString();
        }

        public GetNotificationsResponse Post(GetNotificationsRequest request)
        {
            request.user = GetUserObject(request.UserAuthId);
            GetNotificationsResponse res = new GetNotificationsResponse();
            res.Notification = new List<NotificationInfo>();
            res.PendingActions = new List<PendingActionAndMeetingInfo>();
            res.MyMeetings = new List<PendingActionAndMeetingInfo>();
            this.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
            try
            {
                string str = string.Format(@"
                                                SELECT notification_id, notification, created_at  
                                                FROM eb_notifications 
                                                WHERE user_id = {0}
                                                AND message_seen ='F'
                                                ORDER BY created_at DESC;", request.UserId);

                string _roles = string.Empty;
                if (request.user.RoleIds != null)
                {
                    _roles = string.Join(",", request.user.RoleIds.ToArray());
                }
                string loc_part = string.Empty;
                if (!request.user.LocationIds.Contains(-1))
                    loc_part = $"AND (COALESCE(APP.eb_loc_id, 0) = 0 OR COALESCE(APP.eb_loc_id, 0) = ANY(string_to_array('{request.user.LocationIds.Join(",")}', ',')::int[]))";

                //if meeting related action is needed in notification list then update this query
                str += string.Format(@"
SELECT
  ACT.*
FROM
  eb_my_actions ACT, eb_approval APP
WHERE 
  ACT.id = APP.eb_my_actions_id AND
  COALESCE(APP.eb_del, 'F') = 'F' AND
  ('{0}' = any(string_to_array(ACT.user_ids, ',')) OR (string_to_array(ACT.role_ids,',')) && (string_to_array('{1}',','))) AND 
  COALESCE(ACT.is_completed, 'F') = 'F' AND 
  COALESCE(ACT.eb_del, 'F') = 'F' AND 
  COALESCE(ACT.hide, 'F') = 'F' 
  {2}
ORDER BY 
  ACT.from_datetime DESC;", request.UserId, _roles, loc_part);

                str += string.Format(@"SELECT 
	A.user_id,A.approved_slot_id,A.eb_meeting_schedule_id,B.meeting_date,B.time_from,B.time_to,
	D.title,D.Description,E.id as meeting_id
	FROM
	(
		SELECT 
			id, user_id,approved_slot_id,eb_meeting_schedule_id
	    FROM
			eb_meeting_slot_participants
	    WHERE  
			eb_del = 'F' and user_id= {0}
	)A
	LEFT JOIN
	(
		SELECT
			id,meeting_date,time_from,time_to,
			meeting_date + time_from datetime_merge
		FROM
			eb_meeting_slots
		GROUP BY
		    id,meeting_date,time_from,time_to,eb_del,is_approved
	    Having 
			eb_del = 'F' and is_approved ='T'
	)B
    ON B.id = A.approved_slot_id
	LEFT JOIN	
	(
		SELECT 
			id,eb_meeting_id,eb_slot_participant_id
	    FROM 
			eb_meeting_participants
		GROUP BY
			id,eb_slot_participant_id,eb_meeting_id,eb_del
		Having eb_del = 'F'
	)C
	ON C.eb_slot_participant_id = A.id
	LEFT JOIN	
	(
		SELECT
			id,title,description
		FROM
			eb_meeting_schedule
		GROUP BY
			id,title,description,eb_del
		Having eb_del = 'F'
	)D
	ON D.id = A.eb_meeting_schedule_id
	LEFT JOIN	
	(
		SELECT
			id,eb_meeting_slots_id
		FROM 
			eb_meetings
		GROUP BY
			id,eb_meeting_slots_id,eb_del
		Having eb_del = 'F'
	)E
	ON E.eb_meeting_slots_id = A.approved_slot_id
	WHERE 
		E.id is not null AND
		B.datetime_merge >= current_timestamp", request.UserId);

                EbDataSet ds = EbConnectionFactory.DataDB.DoQueries(str);
                EbDataTable dt = ds.Tables[0];
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    string notif = dt.Rows[i]["notification"].ToString();
                    Notifications list = JsonConvert.DeserializeObject<Notifications>(notif);
                    DateTime created_dtime = Convert.ToDateTime(dt.Rows[i]["created_at"]);
                    var duration = created_dtime.TimeAgo();
                    var _date = created_dtime.ConvertFromUtc(request.user.Preference.TimeZone).ToString(request.user.Preference.GetShortDatePattern() + " " + request.user.Preference.GetShortTimePattern());
                    res.Notification.Add(new NotificationInfo
                    {
                        Link = list.Notification[0].Link,
                        NotificationId = list.Notification[0].NotificationId,
                        Title = list.Notification[0].Title,
                        Duration = duration,
                        CreatedDate = _date
                    });
                }
                dt = ds.Tables[1];
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    var _date = Convert.ToDateTime(dt.Rows[i]["from_datetime"]);
                    var _time = _date.TimeAgo();
                    res.PendingActions.Add(new PendingActionAndMeetingInfo
                    {
                        Description = dt.Rows[i]["description"].ToString(),
                        Link = dt.Rows[i]["form_ref_id"].ToString(),
                        DataId = dt.Rows[i]["form_data_id"].ToString(),
                        CreatedDate = _date.ConvertFromUtc(request.user.Preference.TimeZone).ToString(request.user.Preference.GetShortDatePattern() + " " + request.user.Preference.GetShortTimePattern()),
                        DateInString = _time,
                        ActionType = dt.Rows[i]["my_action_type"].ToString(),
                        MyActionId = Convert.ToInt32(dt.Rows[i]["id"])
                    });
                }
                dt = ds.Tables[2];
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    var _date1 = Convert.ToDateTime(dt.Rows[i]["meeting_date"]);
                    TimeSpan time = TimeSpan.Parse(dt.Rows[i]["time_from"].ToString());
                    _date1 = _date1.Add(time);
                    var _date2 = Convert.ToDateTime(dt.Rows[i]["meeting_date"]);
                    time = TimeSpan.Parse(dt.Rows[i]["time_to"].ToString());
                    _date2 = _date2.Add(time);
                    string _date = _date1.ConvertFromUtc(request.user.Preference.TimeZone).ToString(request.user.Preference.GetShortDatePattern());
                    string time1 = _date1.ToString(request.user.Preference.GetShortTimePattern());
                    string time2 = _date2.ToString(request.user.Preference.GetShortTimePattern());
                    _date = _date + ": " + time1 + " - " + time2;
                    res.MyMeetings.Add(new PendingActionAndMeetingInfo
                    {
                        Description = dt.Rows[i]["title"].ToString(),
                        CreatedDate = _date,
                        //DateInString = _time,
                        MyActionId = Convert.ToInt32(dt.Rows[i]["meeting_id"])
                    });
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Pending Action -----" + e.Message);
                Console.WriteLine("Pending Action -----" + e.StackTrace);
            }
            return res;
        }

        public GetAllActionsResponse Post(GetAllActionsRequest request)
        {
            User _user = GetUserObject(request.UserAuthId);
            string _roles = string.Empty;
            if (_user.RoleIds != null)
            {
                _roles = string.Join(",", _user.RoleIds.ToArray());
            }
            string loc_part = string.Empty;
            if (!_user.LocationIds.Contains(-1))
                loc_part = $"AND (COALESCE(APP.eb_loc_id, 0) = 0 OR COALESCE(APP.eb_loc_id, 0) = ANY(string_to_array('{_user.LocationIds.Join(",")}', ',')::int[]))";

            string str = string.Format(@"
SELECT
  ACT.*, USR.fullname
FROM
  eb_my_actions ACT, eb_approval APP
LEFT JOIN 
  eb_users USR ON USR.id = COALESCE(APP.eb_lastmodified_by, APP.eb_created_by)
WHERE 
  ACT.id = APP.eb_my_actions_id AND
  COALESCE(APP.eb_del, 'F') = 'F' AND
  ('{0}' = any(string_to_array(ACT.user_ids, ',')) OR (string_to_array(ACT.role_ids,',')) && (string_to_array('{1}',','))) AND 
  COALESCE(ACT.is_completed, 'F') = 'F' AND 
  COALESCE(ACT.eb_del, 'F') = 'F' AND 
  COALESCE(ACT.hide, 'F') = 'F' 
  {2}
ORDER BY 
  ACT.from_datetime DESC;", _user.UserId, _roles, loc_part);

            EbDataTable dt = EbConnectionFactory.DataDB.DoQuery(str);
            List<PendingActionAndMeetingInfo> PendingActions = new List<PendingActionAndMeetingInfo>();

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                var _date = Convert.ToDateTime(dt.Rows[i]["from_datetime"]);
                var _time = _date.TimeAgo();
                PendingActions.Add(new PendingActionAndMeetingInfo
                {
                    Description = dt.Rows[i]["description"].ToString(),
                    Link = dt.Rows[i]["form_ref_id"].ToString(),
                    DataId = dt.Rows[i]["form_data_id"].ToString(),
                    CreatedDate = _date.ConvertFromUtc(_user.Preference.TimeZone).ToString(_user.Preference.GetShortDatePattern() + " " + _user.Preference.GetShortTimePattern()),
                    DateInString = _time,
                    ActionType = dt.Rows[i]["my_action_type"].ToString(),
                    MyActionId = Convert.ToInt32(dt.Rows[i]["id"]),
                    CreatedBy = dt.Rows[i]["fullname"].ToString()
                });
            }
            return new GetAllActionsResponse() { PendingActions = PendingActions };
        }

        public GetAllNotificationsResponse Post(GetAllNotificationsRequest request)
        {
            string str = string.Format(@"
SELECT 
  notification_id, notification, created_at  
FROM 
  eb_notifications 
WHERE 
  user_id = {0} AND 
  message_seen ='F'
ORDER BY 
  created_at DESC;", request.UserId);

            User _user = GetUserObject(request.UserAuthId);
            List<NotificationInfo> Notifications = new List<NotificationInfo>();
            EbDataTable dt = EbConnectionFactory.DataDB.DoQuery(str);

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string notif = dt.Rows[i]["notification"].ToString();
                Notifications list = JsonConvert.DeserializeObject<Notifications>(notif);
                DateTime created_dtime = Convert.ToDateTime(dt.Rows[i]["created_at"]);
                var duration = created_dtime.TimeAgo();
                var _date = created_dtime.ConvertFromUtc(_user.Preference.TimeZone).ToString(_user.Preference.GetShortDatePattern() + " " + _user.Preference.GetShortTimePattern());
                Notifications.Add(new NotificationInfo
                {
                    Link = list.Notification[0].Link,
                    NotificationId = list.Notification[0].NotificationId,
                    Title = list.Notification[0].Title,
                    Duration = duration,
                    CreatedDate = _date
                });
            }

            return new GetAllNotificationsResponse() { Notification = Notifications };
        }
    }
}
