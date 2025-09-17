using ExpressBase.Common;
using ExpressBase.Objects;
using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using ServiceStack;
using System.Linq;
using System.ComponentModel; 
using ExpressBase.Objects.Objects.SmsRelated;
using ExpressBase.Common.Structures;

namespace ExpressBase.ServiceStack.Services
{
    public class ExportAppService : EbBaseService
    {
        public ExportAppService(IEbConnectionFactory _dbf) : base(_dbf) { }

        EbConnectionFactory ebConnectionFactory;
        EbObjectService objservice;

        [Authenticate]
        public DependancyMatrixResponse Post(DependancyMatrixRequest request)
        {
            this.ebConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
            this.objservice = base.ResolveService<EbObjectService>();
            this.objservice.EbConnectionFactory = ebConnectionFactory;

            DependancyMatrixResponse resp = new DependancyMatrixResponse();
            List<Dominant> dominants = new List<Dominant>();
            List<string> relatedobjects;

            foreach (var app in request.AppObjectsMap)
            {
                foreach (var c in app.Value)
                {
                    foreach (var o in c.Value)
                    {
                        FillDominants(dominants, o.RefId, o.DisplayName);
                    }
                }
            }

            resp.Dominants = dominants.OrderBy(d => d.DisplayName).ToList();

            return resp;
        }
        public void FillDominants(List<Dominant> dominants, string refid, string displayName)
        {
            KeyValuePair<string, List<string>> relatedobjects = GetRelatedRefids(refid);
            if (relatedobjects.Key != string.Empty)
            {
                Dominant existingEntry = dominants.FirstOrDefault(d => d.RefId == refid);
                if (existingEntry?.Dependents != null)
                {
                    List<string> newDependents = relatedobjects.Value.Except(existingEntry.Dependents).ToList();
                    if (newDependents?.Count > 0)
                    {
                        existingEntry.Dependents.AddRange(newDependents);
                    }
                    else
                    {
                        return;
                    }
                }
                else
                {
                    if (!dominants.Any(d => d.RefId == refid))
                        dominants.Add(new Dominant
                        {
                            DisplayName = relatedobjects.Key,
                            RefId = refid,
                            Dependents = relatedobjects.Value
                        });
                }

                if (relatedobjects.Value?.Count > 0)
                {
                    foreach (string reference in relatedobjects.Value)
                    {
                        if (!string.IsNullOrWhiteSpace(reference))
                        {
                            FillDominants(dominants, reference, displayName);
                        }
                    }
                };
            }
        }

        public EbObject GetObjfromDB(string refid)
        {
            EbObjectParticularVersionResponse res = (EbObjectParticularVersionResponse)objservice.Get(new EbObjectParticularVersionRequest { RefId = refid });
            EbObject obj = (res.Data.Count > 0) ? EbSerializers.Json_Deserialize(res.Data[0].Json) : null;

            return obj;
        }

        public KeyValuePair<string, List<string>> GetRelatedRefids(string refid)
        {
            EbObject obj = GetObjectFromRedis(refid) ?? GetObjfromDB(refid);

            if (obj == null)
            {
                Console.WriteLine("Object not in db : reference error " + refid);
                return new KeyValuePair<string, List<string>>("", new List<string>());
            }

            return new KeyValuePair<string, List<string>>(obj.DisplayName, obj.DiscoverRelatedRefids());
        }

        public EbObject GetObjectFromRedis(string refId)
        {
            EbObject obj = null;
            int type = Convert.ToInt32(refId.Split('-')[2]);

            if (type == EbObjectTypes.FilterDialog.IntCode)
                obj = Redis.Get<EbFilterDialog>(refId);
            else if (type == EbObjectTypes.Report.IntCode)
                obj = Redis.Get<EbReport>(refId);
            else if (type == EbObjectTypes.DataReader.IntCode)
                obj = Redis.Get<EbDataReader>(refId);
            else if (type == EbObjectTypes.DataWriter.IntCode)
                obj = Redis.Get<EbDataWriter>(refId);
            else if (type == EbObjectTypes.ChartVisualization.IntCode)
                obj = Redis.Get<EbChartVisualization>(refId);
            else if (type == EbObjectTypes.CalendarView.IntCode)
                obj = Redis.Get<EbCalendarView>(refId);
            else if (type == EbObjectTypes.TableVisualization.IntCode)
                obj = Redis.Get<EbTableVisualization>(refId);
            else if (type == EbObjectTypes.MapView.IntCode)
                obj = Redis.Get<Objects.EbMapView>(refId);
            else if (type == EbObjectTypes.WebForm.IntCode)
                obj = Redis.Get<EbWebForm>(refId);
            else if (type == EbObjectTypes.UserControl.IntCode)
                obj = Redis.Get<EbUserControl>(refId);
            else if (type == EbObjectTypes.BotForm.IntCode)
                obj = Redis.Get<EbBotForm>(refId);
            else if (type == EbObjectTypes.EmailBuilder.IntCode)
                obj = Redis.Get<EbEmailTemplate>(refId);
            else if (type == EbObjectTypes.SmsBuilder.IntCode)
                obj = Redis.Get<EbSmsTemplate>(refId);
            else if (type == EbObjectTypes.SqlFunction.IntCode)
                obj = Redis.Get<EbSqlFunction>(refId);
            else if (type == EbObjectTypes.Api.IntCode)
                obj = Redis.Get<EbApi>(refId);
            else if (type == EbObjectTypes.DashBoard.IntCode)
                obj = Redis.Get<EbDashBoard>(refId);
            else if (type == EbObjectTypes.MobilePage.IntCode)
                obj = Redis.Get<EbMobilePage>(refId);
            else if (type == EbObjectTypes.SqlJob.IntCode)
                obj = Redis.Get<EbSqlJob>(refId);
            else if (type == EbObjectTypes.HtmlPage.IntCode)
                obj = Redis.Get<EbHtmlPage>(refId);
            else if (type == EbObjectTypes.MaterializedView.IntCode)
                obj = Redis.Get<EbMaterializedView>(refId);

            return obj;
        }
    }
}
