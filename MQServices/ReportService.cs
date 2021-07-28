using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Security;
using ExpressBase.ServiceStack.Services;
using Newtonsoft.Json;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.MQServices
{
    [Restrict(InternalOnly = true)]
    public class ReportInternalService : EbMqBaseService
    {
        public ReportInternalService(IServiceClient _ssclient, IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_ssclient, _mqp, _mqc)
        {
            ObjectService = base.ResolveService<EbObjectService>();
            ReportService = base.ResolveService<ReportService>();
            SchedulersService = base.ResolveService<SchedulerServices>();
        }

        EbObjectService ObjectService { get; set; }

        ReportService ReportService { get; set; }

        SchedulerServices SchedulersService { get; set; }

        ReportRenderResponse RepRes = null;

        EbJobArguments JobArgs = null;

        AllGroupCollection GroupCollection { get; set; }
        AllUserCollection UserCollection { get; set; }

        AllDelMessagaeCollection MessageCollection { get; set; }

        Dictionary<string, List<User>> Locales = new Dictionary<string, List<User>>();
        //Dictionary<string, List<User>> SlackLocales = new Dictionary<string, List<User>>();
        public ReportInternalResponse Post(ReportInternalRequest request)
        {
            try
            {
                Console.WriteLine(" Reached Inside MQService/ReportServiceInternal in SS .. Before Report Render");
                JobArgs = request.JobArgs;
                EbConnectionFactory ebConnectionFactory = new EbConnectionFactory(JobArgs.SolnId, this.Redis);
                ObjectService.EbConnectionFactory = ebConnectionFactory;
                ReportService.EbConnectionFactory = ebConnectionFactory;
                SchedulersService.EbConnectionFactory = ebConnectionFactory;

                UserCollection = JsonConvert.DeserializeObject<AllUserCollection>(JobArgs.ToUserIds);
                GroupCollection = JsonConvert.DeserializeObject<AllGroupCollection>(JobArgs.ToUserGroupIds);
                MessageCollection = JsonConvert.DeserializeObject<AllDelMessagaeCollection>(JobArgs.Message);


                Dictionary<string, byte[]> LocaleReport = new Dictionary<string, byte[]>();
                Console.WriteLine(" Fetching Live version  " + JobArgs.ObjId);

                EbObjectFetchLiveVersionResponse res = ((EbObjectFetchLiveVersionResponse)ObjectService.Get(new EbObjectFetchLiveVersionRequest()
                {
                    Id = JobArgs.ObjId
                }));


                if (res.Data != null && res.Data.Count > 0)
                {


                    if (UserCollection.EmailUser != "" || GroupCollection.EmailGroup != "")
                    {
                        Locales = new Dictionary<string, List<User>>();
                        JobArgs.DeliveryMechanisms = (DeliveryMechanisms)1;
                        JobArgs.Message = MessageCollection.EmailMessage;
                        GetEmailUserDetails(JobArgs);
                        JobPush(res);
                    }
                    if (UserCollection.SMSUser != "" || GroupCollection.SMSGroup != "")
                    {
                        JobArgs.DeliveryMechanisms = (DeliveryMechanisms)2;
                        JobArgs.Message = MessageCollection.SMSMessage;
                        JobPush(res);
                    }
                    if (UserCollection.SlackUser != "" || GroupCollection.SlackGroup != "")
                    {
                        Locales = new Dictionary<string, List<User>>();
                        JobArgs.DeliveryMechanisms = (DeliveryMechanisms)3;
                        JobArgs.Message = MessageCollection.SlackMessage;
                        getSlackUser();
                        JobPush(res);
                    }

                }
                else
                {
                    Console.WriteLine("No Live version avaialble for :" + JobArgs.ObjId);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("     Error in ReportInternal " + e.Message + e.StackTrace);
            }
            return new ReportInternalResponse() { };
        }

        public void JobPush(EbObjectFetchLiveVersionResponse res)
        {
            try
            {
                EbObjectWrapper Live_ver = res.Data[0];
                foreach (KeyValuePair<string, List<User>> locale in Locales)
                {
                    RepRes = ReportService.Get(new ReportRenderRequest
                    {
                        Refid = Live_ver.RefId,
                        RenderingUser = new User { FullName = "Machine User" },
                        ReadingUser = locale.Value[0],
                        Params = JobArgs.Params,
                        SolnId = JobArgs.SolnId,
                        UserAuthId = JobArgs.UserAuthId,
                        UserId = JobArgs.UserId,
                        WhichConsole = "uc"
                    });
                    Console.WriteLine(locale.Key);
                    Console.WriteLine("Inside MQService/ReportServiceInternal in SS \n After Report Render .Going to send email");
                    if (RepRes != null)
                    {
                        RepRes.StreamWrapper.Memorystream.Position = 0;
                        foreach (User _u in locale.Value)
                        {
                            if (JobArgs.DeliveryMechanisms == DeliveryMechanisms.Email)
                                PublishMail(_u.Email);
                            else if (JobArgs.DeliveryMechanisms == DeliveryMechanisms.Slack)
                                PublishSlackChat(_u.Email);
                            //else if (JobArgs.DeliveryMechanisms == DeliveryMechanisms.Sms)
                            //    PublishSMS(_u.Email);
                        }
                    }
                    //LocaleReport.Add(locale.Key, RepRes.ReportBytea);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("     Error in ReportInternal " + e.Message + e.StackTrace);
            }
        }

        public void GetEmailUserDetails(EbJobArguments JobArgs)
        {

            Console.WriteLine("----------Got Live Version   Getting mail ids");
            GetUserEmailsResponse mailres = SchedulersService.Get(new GetUserEmailsRequest()
            {
                UserIds = UserCollection.EmailUser,
                UserGroupIds = GroupCollection.EmailGroup
            });
            Dictionary<int, string> MailIds = mailres.UserEmails
                .Concat(mailres.UserGroupEmails)
                .GroupBy(d => d.Key)
                .ToDictionary(d => d.Key, d => d.First().Value);

            foreach (KeyValuePair<int, string> u in MailIds)
            {
                User usr = GetUserObject(string.Format(TokenConstants.SUB_FORMAT, JobArgs.SolnId, u.Key, "uc"));
                if (usr != null)
                {
                    if (Locales.ContainsKey(usr.Preference.Locale))
                        Locales[usr.Preference.Locale].Add(usr);
                    else Locales.Add(usr.Preference.Locale, new List<User> { usr });
                }
                else
                {
                    Console.WriteLine("Redis User Object is empty for : " + u.Value);
                }
            }
            Console.WriteLine("Number of locales : " + Locales.Count());
        }

        public void getSlackUser()
        {

            List<User> Users = new List<User>();
            string[] _slackUsers = (UserCollection.SlackUser + "," + UserCollection.SlackUser).Split(',').Distinct().ToArray();
            _slackUsers = (GroupCollection.SlackGroup + "," + GroupCollection.SlackGroup).Split(',').Distinct().ToArray();
            foreach (string slackid in _slackUsers)
            {
                User _u = new User { Email = slackid, Preference = new Preferences { TimeZone = "(UTC+05:30) Chennai, Kolkata, Mumbai, New Delhi", Locale = "en-US" } };
                Users.Add(_u);
            }
            Locales.Add("en-US", Users);
        }

        public void PublishMail(string _mailId)
        {
            MessageProducer3.Publish(new EmailServicesRequest()
            {
                From = "request.from",
                To = _mailId,
                Cc = /*ebEmailTemplate.Cc.Split(",")*/ null,
                Bcc = /*ebEmailTemplate.Bcc.Split(",")*/ null,
                Subject = RepRes.ReportName + " - " + RepRes.CurrentTimestamp.ToShortDateString(),
                Message = JobArgs.Message,
                UserId = JobArgs.UserId,
                UserAuthId = JobArgs.UserAuthId,
                SolnId = JobArgs.SolnId,
                AttachmentReport = RepRes.ReportBytea,
                AttachmentName = RepRes.ReportName + " - " + RepRes.CurrentTimestamp.ToString("dd-MM-yy") + ".pdf"
            });
            Console.WriteLine("Email to " + _mailId + " pushed.....");
        }
        public void PublishSlackChat(string _to)
        {
            MessageProducer3.Publish(new SlackCreateRequest
            {
                ObjId = JobArgs.ObjId,
                Params = JobArgs.Params,
                SolnId = JobArgs.SolnId,
                UserId = JobArgs.UserId,
                UserAuthId = JobArgs.UserAuthId,
                AttachmentReport = RepRes.ReportBytea,
                AttachmentName = RepRes.ReportName + " - " + RepRes.CurrentTimestamp.ToString("dd-MM-yy"),
                Message = JobArgs.Message,
                To = _to
            });
        }

        public void PublishSMS(string _to)
        {
            MessageProducer3.Publish(new SMSPrepareRequest
            {
                ObjId = JobArgs.ObjId,
                Params = JobArgs.Params,
                SolnId = JobArgs.SolnId,
                UserId = JobArgs.UserId,
                UserAuthId = JobArgs.UserAuthId,
                Message = JobArgs.Message,
                To = _to
            });
        }
    }
}
