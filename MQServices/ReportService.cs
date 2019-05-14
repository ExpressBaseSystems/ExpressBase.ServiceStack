using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Security;
using ExpressBase.ServiceStack.Services;
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
        public ReportInternalService(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }
        public ReportInternalResponse Post(ReportInternalRequest request)
        {
            Console.WriteLine("Inside MQService/ReportServiceInternal in SS \n Before Report Render");
            EbConnectionFactory ebConnectionFactory = new EbConnectionFactory(request.JobArgs.SolnId, this.Redis);
            var objservice = base.ResolveService<EbObjectService>();
            objservice.EbConnectionFactory = ebConnectionFactory;
            var reportservice = base.ResolveService<ReportService>();
            reportservice.EbConnectionFactory = ebConnectionFactory;
            var schedulerservice = base.ResolveService<SchedulerServices>();
            schedulerservice.EbConnectionFactory = ebConnectionFactory;
            Dictionary<string, List<User>> LocaleUser = new Dictionary<string, List<User>>();
            Dictionary<string, byte[]> LocaleReport = new Dictionary<string, byte[]>();
            EbObjectFetchLiveVersionResponse res = ((EbObjectFetchLiveVersionResponse)objservice.Get(new EbObjectFetchLiveVersionRequest()
            {
                Id = request.JobArgs.ObjId
            }));
            if (res.Data.Count > 0)
            {
                EbObjectWrapper Live_ver = res.Data[0];
                GetUserEmailsResponse mailres = schedulerservice.Get(new GetUserEmailsRequest()
                {
                    UserIds = request.JobArgs.ToUserIds,
                    UserGroupIds = request.JobArgs.ToUserGroupIds
                });
                var MailIds = mailres.UserEmails
                    .Concat(mailres.UserGroupEmails)
                    .GroupBy(d => d.Key)
                    .ToDictionary(d => d.Key, d => d.First().Value);

                foreach (var u in MailIds)
                {
                    User usr = this.Redis.Get<User>(string.Format(TokenConstants.SUB_FORMAT, request.JobArgs.SolnId, u.Value, "uc"));
                    if (usr != null)
                    {
                        if (LocaleUser.ContainsKey(usr.Preference.Locale))
                            LocaleUser[usr.Preference.Locale].Add(usr);
                        else LocaleUser.Add(usr.Preference.Locale, new List<User> { usr });
                    }
                }
                foreach (var locale in LocaleUser)
                {
                    ReportRenderResponse RepRes = reportservice.Get(new ReportRenderRequest
                    {
                        Refid = Live_ver.RefId,
                        RenderingUser = new User { FullName = "Machine User" },
                        ReadingUser = locale.Value[0],
                        Params = request.JobArgs.Params,
                        SolnId = request.JobArgs.SolnId,
                        UserAuthId = request.JobArgs.UserAuthId,
                        UserId = request.JobArgs.UserId,
                        WhichConsole = "uc"
                    });
                    Console.WriteLine(locale.Key);
                    Console.WriteLine("Inside MQService/ReportServiceInternal in SS \n After Report Render");
                    RepRes.StreamWrapper.Memorystream.Position = 0;
                    foreach (var _u in locale.Value)
                    {
                        MessageProducer3.Publish(new EmailServicesRequest()
                        {
                            From = "request.from",
                            To = _u.Email,
                            Cc = /*ebEmailTemplate.Cc.Split(",")*/ null,
                            Bcc = /*ebEmailTemplate.Bcc.Split(",")*/ null,
                            Subject = RepRes.ReportName + " - " + RepRes.CurrentTimestamp.ToShortDateString(),
                            Message = "<div>Hi, </div><div>&nbsp;The report " + RepRes.ReportName + " generated on " +
                            RepRes.CurrentTimestamp.ToShortDateString() + " at " + RepRes.CurrentTimestamp.ToShortTimeString() + ". Please find the attachment for the same. </div><div><br>Thanks.<br></div>",
                            UserId = request.JobArgs.UserId,
                            UserAuthId = request.JobArgs.UserAuthId,
                            SolnId = request.JobArgs.SolnId,
                            AttachmentReport = RepRes.ReportBytea,
                            AttachmentName = RepRes.ReportName + " - " + RepRes.CurrentTimestamp.ToString("dd-MM-yy")
                        });
                    }
                    //LocaleReport.Add(locale.Key, RepRes.ReportBytea);
                }



            }
            return new ReportInternalResponse() { };
        }
    }
}
