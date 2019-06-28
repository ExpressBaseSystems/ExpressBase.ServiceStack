using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Objects;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Objects;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.ServiceStack.Services;
using Newtonsoft.Json;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.MQServices
{
    [Authenticate]
    public class ImportrExportService : EbMqBaseService
    {
        public ImportrExportService(IMessageProducer _mqp, IMessageQueueClient _mqc, IEbServerEventClient _sec) : base(_mqp, _mqc, _sec)
        {
        }

        public ExportApplicationResponse Post(ExportApplicationMqRequest request)
        {

            ExportApplicationResponse resp = new ExportApplicationResponse();
            MessageProducer3.Publish(new ExportApplicationRequest
            {
                AppId = request.AppId,
                Refids = request.Refids,
                BToken = this.ServerEventClient.BearerToken,
                RToken = this.ServerEventClient.RefreshToken,
                SolnId = request.SolnId,
                UserId = request.UserId,
                UserAuthId = request.UserAuthId,
                WhichConsole = request.WhichConsole
            });
            Log.Info("ExportApplicationRequest published to Mq");
            return resp;
        }
        public ImportApplicationResponse Get(ImportApplicationMqRequest request)
        {
            ImportApplicationResponse resp = new ImportApplicationResponse();
            this.MessageProducer3.Publish(new ImportApplicationRequest
            {
                Id = request.Id,
                BToken = this.ServerEventClient.BearerToken,
                RToken = this.ServerEventClient.RefreshToken,
                SolnId = request.SolnId,
                UserId = request.UserId,
                UserAuthId = request.UserAuthId,
                WhichConsole = request.WhichConsole
            });
            Log.Info("ImportApplicationRequest published to Mq");
            return resp;
        }
    }

    [Restrict(InternalOnly = true)]
    public class ImportExportInternalService : EbMqBaseService
    {
        public ImportExportInternalService() : base() { }

        public string Post(ExportApplicationRequest request)
        {
            Log.Info("ExportApplicationRequest inside Mq");
            OrderedDictionary ObjDictionary = new OrderedDictionary();
            try
            {
                EbConnectionFactory ebConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
                var devservice = base.ResolveService<DevRelatedServices>();
                devservice.EbConnectionFactory = ebConnectionFactory;
                var appstoreService = base.ResolveService<AppStoreService>();
                appstoreService.EbConnectionFactory = ebConnectionFactory;
                AppWrapper AppObj = devservice.Get(new GetApplicationRequest { Id = request.AppId }).AppInfo;
                AppObj.ObjCollection = new List<EbObject>();

                string[] refs = request.Refids.Split(",");
                foreach (string _refid in refs)
                    GetRelated(_refid, ObjDictionary, request.SolnId);

                ICollection ObjectList = ObjDictionary.Values;
                foreach (object item in ObjectList)
                    AppObj.ObjCollection.Add(item as EbObject);
                string appobj_s = EbSerializers.Json_Serialize4AppWraper(AppObj);
                SaveToAppStoreResponse p = appstoreService.Post(new SaveToAppStoreRequest
                {
                    Store = new AppStore
                    {
                        Name = AppObj.Name,
                        Cost = 10.00m,
                        Currency = "USD",
                        Json = appobj_s,
                        Status = 1,
                        AppType = 1,
                        Description = AppObj.Description,
                        Icon = AppObj.Icon
                    },
                    SolnId = request.SolnId,
                    UserId = request.UserId,
                    UserAuthId = request.UserAuthId,
                    WhichConsole = request.WhichConsole
                });
                Console.WriteLine("ExportApplication success");
            }
            catch (Exception e)
            {
                Console.WriteLine("ExportApplication" + e.Message);
            }

            return null;
        }

        public string Post(ImportApplicationRequest request)
        {
            Log.Info("ImportApplicationRequest inside Mq");
            Dictionary<string, string> RefidMap = new Dictionary<string, string>();
            try
            {
                EbConnectionFactory _ebConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
                var appstoreService = base.ResolveService<AppStoreService>();
                appstoreService.EbConnectionFactory = _ebConnectionFactory;
                var devservice = base.ResolveService<DevRelatedServices>();
                devservice.EbConnectionFactory = _ebConnectionFactory;
                var objservice = base.ResolveService<EbObjectService>();
                objservice.EbConnectionFactory = _ebConnectionFactory;

                AppWrapper AppObj = appstoreService.Get(new GetOneFromAppStoreRequest
                {
                    Id = request.Id,
                    SolnId = request.SolnId,
                    UserAuthId = request.UserAuthId,
                    UserId = request.UserId,
                    WhichConsole = request.WhichConsole,
                }).Wrapper;
                List<EbObject> ObjectCollection = AppObj.ObjCollection;
                UniqueApplicationNameCheckResponse uniq_appnameresp;
                do
                {
                    uniq_appnameresp = devservice.Get(new UniqueApplicationNameCheckRequest { AppName = AppObj.Name });
                    if (!uniq_appnameresp.IsUnique)
                        AppObj.Name = AppObj.Name + "(1)";
                }
                while (!uniq_appnameresp.IsUnique);
                CreateApplicationResponse appres = devservice.Post(new CreateApplicationRequest
                {
                    AppName = AppObj.Name,
                    AppType = AppObj.AppType,
                    Description = AppObj.Description,
                    AppIcon = AppObj.Icon
                });

                for (int i = ObjectCollection.Count - 1; i >= 0; i--)
                {
                    UniqueObjectNameCheckResponse uniqnameresp;
                    EbObject obj = ObjectCollection[i];

                    do
                    {
                        uniqnameresp = objservice.Get(new UniqueObjectNameCheckRequest { ObjName = obj.Name });
                        if (!uniqnameresp.IsUnique)
                            obj.Name = obj.Name + "(1)";
                    }
                    while (!uniqnameresp.IsUnique);


                    EbObject_Create_New_ObjectRequest ds = new EbObject_Create_New_ObjectRequest
                    {
                        Name = obj.Name,
                        DisplayName = obj.DisplayName,
                        Description = obj.Description,
                        Json = EbSerializers.Json_Serialize(obj),
                        Status = ObjectLifeCycleStatus.Dev,
                        Relations = "_rel_obj",
                        IsSave = false,
                        Tags = "_tags",
                        Apps = appres.Id.ToString(),
                        SourceSolutionId = (obj.RefId.Split("-"))[0],
                        SourceObjId = (obj.RefId.Split("-"))[3],
                        SourceVerID = (obj.RefId.Split("-"))[4],
                        SolnId = request.SolnId,
                        UserId = request.UserId,
                        UserAuthId = request.UserAuthId,
                        WhichConsole = request.WhichConsole
                    };
                    EbObject_Create_New_ObjectResponse res = objservice.Post(ds);
                    RefidMap[obj.RefId] = res.RefId;

                    // obj.ReplaceRefid(RefidMap);
                }
                for (int i = ObjectCollection.Count - 1; i >= 0; i--)
                {
                    EbObject obj = ObjectCollection[i];
                    obj.ReplaceRefid(RefidMap);
                    EbObject_SaveRequest ss = new EbObject_SaveRequest
                    {
                        RefId = RefidMap[obj.RefId],
                        Name = obj.Name,
                        DisplayName = obj.DisplayName,
                        Description = obj.Description,
                        Json = EbSerializers.Json_Serialize(obj),
                        Apps = appres.Id.ToString(),
                        SolnId = request.SolnId,
                        UserId = request.UserId,
                        UserAuthId = request.UserAuthId,
                        WhichConsole = request.WhichConsole,
                        Relations = "_rel_obj",
                        Tags = "_tags"
                    };
                    EbObject_SaveResponse saveRes = objservice.Post(ss);
                }
                Console.WriteLine("ImportApplication success");
            }
            catch (Exception e)
            {
                Console.WriteLine("ImportApplication" + e.Message);
            }
            return null;
        }
        public void GetRelated(string _refid, OrderedDictionary ObjDictionary, string solid)
        {
            EbObject obj = null;

            //if (ObjDictionary.Contains(_refid))
            //{
            //    obj = (EbObject)ObjDictionary[_refid];
            //    ObjDictionary.Remove(_refid);
            //}
            //else
            //    obj = GetObjfromDB(_refid, solid);
            if (!ObjDictionary.Contains(_refid))
            {
                obj = GetObjfromDB(_refid, solid);
                ObjDictionary.Add(_refid, obj);
                string RefidS = obj.DiscoverRelatedRefids();

                string[] _refCollection = RefidS.Split(",");
                foreach (string _ref in _refCollection)
                    if (_ref.Trim() != string.Empty)
                        GetRelated(_ref, ObjDictionary, solid);
            }
        }
        public EbObject GetObjfromDB(string _refid, string solid)
        {
            EbConnectionFactory ebConnectionFactory = new EbConnectionFactory(solid, this.Redis);
            EbObjectService objservice = base.ResolveService<EbObjectService>();
            objservice.EbConnectionFactory = ebConnectionFactory;
            EbObjectParticularVersionResponse res = (EbObjectParticularVersionResponse)objservice.Get(new EbObjectParticularVersionRequest { RefId = _refid });
            EbObject obj = EbSerializers.Json_Deserialize(res.Data[0].Json);
            obj.RefId = _refid;
            return obj;
        }
    }
}
