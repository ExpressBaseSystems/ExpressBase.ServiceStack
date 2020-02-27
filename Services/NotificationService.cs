using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ExpressBase.Common.ServerEvents_Artifacts;
using ExpressBase.Common.Constants;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Data;
using ServiceStack;
using System.Text;
using System.Data.Common;
using ExpressBase.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace ExpressBase.ServiceStack.Services
{
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
                    this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
                    {
                        Msg = JsonConvert.SerializeObject(n),
                        Selector = "cmd.onNotification",
                        ToUserAuthId = request.UserAuthId,
                        NotificationId = notification_id,
                        NotifyUserId = request.UsersID
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

        public NotifyByUserRoleResponse Post(NotifyByUserRoleRequest request)
        {
            NotifyByUserRoleResponse res = new NotifyByUserRoleResponse();
            Notifications n = new Notifications();
            List<NotificationInfo> Notification = new List<NotificationInfo>();
            List<int> user_id = new List<int>();
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
                        string str = string.Format(@" select user_id from eb_role2user where role_id = '{0}'", role_id);
                        EbDataTable dt = EbConnectionFactory.DataDB.DoQuery(str);
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            if (!user_id.Contains(int.Parse(dt.Rows[i][0].ToString())))
                                user_id.Add(int.Parse(dt.Rows[i][0].ToString()));
                        }
                    }
                    this.ServerEventClient.Post<NotifyResponse>(new NotifyUsersRequest
                    {
                        Msg = JsonConvert.SerializeObject(n),
                        Selector = "cmd.onNotification",
                        ToUserAuthId = request.UserAuthId,
                        NotificationId = notification_id,
                        UsersId = user_id
                    });
                }
                else
                {
                    throw new Exception("Notification Title or Link Empty");
                }
            }
            catch(Exception e)
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
            List<int> user_id = new List<int>();
            try
            {
                if(request.Link != null && request.Title != null)
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
                        string str = string.Format(@" select userid from eb_user2usergroup where groupid ='{0}'", grp_id);
                        EbDataTable dt = EbConnectionFactory.DataDB.DoQuery(str);
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            if (!user_id.Contains(int.Parse(dt.Rows[i][0].ToString())))
                                user_id.Add(int.Parse(dt.Rows[i][0].ToString()));
                        }
                    }
                    this.ServerEventClient.Post<NotifyResponse>(new NotifyUsersRequest
                    {
                        Msg = JsonConvert.SerializeObject(n),
                        Selector = "cmd.onNotification",
                        ToUserAuthId = request.UserAuthId,
                        NotificationId = notification_id,
                        UsersId = user_id
                    });
                }
                else
                {
                    throw new Exception("Notification Title or Link Empty");
                }
            }
            catch(Exception e)
            {
                throw e;
            }
            return res;
        }

        public GetNotificationFromDbResponse Post(GetNotificationFromDbRequest request)
        {
            GetNotificationFromDbResponse res = new GetNotificationFromDbResponse();
            List <NotificationInfo> n = new List<NotificationInfo>();
            this.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
            using (DbConnection con = this.EbConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                string str = string.Format(@"UPDATE eb_notifications SET message_seen = 'T' WHERE notification_id = '{0}' AND user_id = '{1}';",
                    request.NotificationId, request.UserId);
                DbCommand cmd = this.EbConnectionFactory.DataDB.GetNewCommand(con, str);
                cmd.ExecuteNonQuery();
            }
            string str1 = string.Format(@"
                                                SELECT notification_id, notification, created_at 
                                                FROM eb_notifications 
                                                WHERE user_id = '{0}'
                                                AND message_seen ='F'", request.UserId);

            EbDataTable dt = EbConnectionFactory.DataDB.DoQuery(str1);

            for(int i=0; i<dt.Rows.Count;i++)
            {
                string notif = dt.Rows[i]["notification"].ToString();
                Notifications list = JsonConvert.DeserializeObject<Notifications>(notif);
                DateTime created_dtime = Convert.ToDateTime(dt.Rows[i]["created_at"].ToString());
                string duration = GetNotificationDuration(created_dtime);
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
            builder.Append( DateTime.Now.ToString("yyyyMMddHHmmssffff"));
            notify_id = builder.ToString();
            return notify_id;
        }

        public static string GenerateStr( Random random)
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
            GetNotificationsResponse res = new GetNotificationsResponse();
            List<NotificationInfo> Notifications = new List<NotificationInfo>();
            this.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
            try
            {
                string str = string.Format(@"
                                                SELECT notification_id, notification, created_at  
                                                FROM eb_notifications 
                                                WHERE user_id = '{0}'
                                                AND message_seen ='F'
                                                ORDER BY created_at DESC;", request.UserId);
            EbDataTable dt = EbConnectionFactory.DataDB.DoQuery(str);
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string notif = dt.Rows[i]["notification"].ToString();
                Notifications list = JsonConvert.DeserializeObject<Notifications>(notif);
                DateTime created_dtime = Convert.ToDateTime(dt.Rows[i]["created_at"].ToString());
                string duration = GetNotificationDuration(created_dtime);
                Notifications.Add(new NotificationInfo
                {
                    Link = list.Notification[0].Link,
                    NotificationId = list.Notification[0].NotificationId,
                    Title = list.Notification[0].Title,
                    Duration = duration
                });
            }
            res.Notifications = Notifications;
            res.PendingActions = Post(new GetPendingActionRequest { user = request.user}).PendingActions;
            }
            catch (Exception e)
            {
                Console.WriteLine("Pending Action -----"+e.Message);
                Console.WriteLine("Pending Action -----" + e.StackTrace);
            }
            return res;
        }

        string GetNotificationDuration(DateTime created_dtime)
        {
            string duration = "Today";
            DateTime now = DateTime.Now;
            if(now.Year > created_dtime.Year)
            {
                int yr = (now.Year - created_dtime.Year);
                duration = (yr>1)? yr + " years ago": yr + " year ago";
            }
            else if(now.Month > created_dtime.Month)
            {
                int mnth = (now.Month - created_dtime.Month);
                duration = (mnth>1)? mnth + " months ago": mnth + " month ago";
            }
            else if(now.Day > created_dtime.Day)
            {
                int day = (now.Day - created_dtime.Day);
                duration = (day>1)? day + " days ago": day + " day ago";
            }
            return duration;
        }

        public GetPendingActionResponse Post(GetPendingActionRequest request)
        {
            GetPendingActionResponse res = new GetPendingActionResponse();
            res.PendingActions = new List<PendingActionInfo>();
            string _roles = string.Join(",", request.user.Roles
                                            .Select(x => string.Format("'{0}'", x)));
            string str = string.Format(@"SELECT *
                    FROM eb_my_actions
                    WHERE '{0}' = any(string_to_array(user_ids, ',')) OR
                     role_id IN(select id from eb_roles where role_name IN({1}));", request.user.UserId, _roles);
            EbDataTable dt = EbConnectionFactory.DataDB.DoQuery(str);
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                res.PendingActions.Add(new PendingActionInfo
                { 
                    Description =  dt.Rows[i]["description"].ToString(),
                    Link = dt.Rows[i]["form_ref_id"].ToString(),
                    DataId = dt.Rows[i]["form_data_id"].ToString(),
                    CreatedDate =Convert.ToDateTime( dt.Rows[i]["from_datetime"]).ToString("dd-MM-yyyy")
                });
            }
            return res;
        }
    }
}
