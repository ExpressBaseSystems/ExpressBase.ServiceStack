using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.LocationNSolution;
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
                //BToken = this.ServerEventClient.BearerToken,
                //RToken = this.ServerEventClient.RefreshToken,
                SolnId = request.SolnId,
                UserId = request.UserId,
                UserAuthId = request.UserAuthId,
                WhichConsole = request.WhichConsole,
                IsDemoApp = request.IsDemoApp,
                SelectedSolutionId = request.SelectedSolutionId
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
                Console.WriteLine("ExportApplication" + e.Message + e.StackTrace);
            }

            return null;
        }

        public string Post(ImportApplicationRequest request)
        {
            Log.Info("ImportApplicationRequest inside Mq");
            Dictionary<string, string> RefidMap = new Dictionary<string, string>();
            try
            {
                EbConnectionFactory _ebConnectionFactory = new EbConnectionFactory(request.SelectedSolutionId, this.Redis);
                var appstoreService = base.ResolveService<AppStoreService>();
                appstoreService.EbConnectionFactory = _ebConnectionFactory;
                var devservice = base.ResolveService<DevRelatedServices>();
                devservice.EbConnectionFactory = _ebConnectionFactory;
                var objservice = base.ResolveService<EbObjectService>();
                objservice.EbConnectionFactory = _ebConnectionFactory;

                AppWrapper AppObj = appstoreService.Get(new GetOneFromAppStoreRequest
                {
                    Id = request.Id,
                    SolnId = request.SelectedSolutionId,
                    UserAuthId = request.UserAuthId,
                    UserId = request.UserId,
                    WhichConsole = request.WhichConsole,
                }).Wrapper;
                if (AppObj != null)
                {
                    List<EbObject> ObjectCollection = AppObj.ObjCollection;
                    if (ObjectCollection.Count > 0)
                    {
                        int c = 0;
                        string _appname = AppObj.IsPublic ? AppObj.Title : AppObj.Name;
                        UniqueApplicationNameCheckResponse uniq_appnameresp;
                        do
                        {
                            c++;
                            uniq_appnameresp = devservice.Get(new UniqueApplicationNameCheckRequest { AppName = _appname });
                            if (!uniq_appnameresp.IsUnique)
                                _appname = _appname + "(" + c + ")";
                        }
                        while (!uniq_appnameresp.IsUnique);

                        CreateApplicationResponse appres = devservice.Post(new CreateApplicationRequest
                        {
                            AppName = _appname,
                            AppType = AppObj.AppType,
                            Description = AppObj.Description,
                            AppIcon = AppObj.Icon
                        });
                        Console.WriteLine("Created application : " + _appname);
                        bool _isVersionedSolution = IsVersioned(request.SelectedSolutionId, request.UserId);
                        for (int i = ObjectCollection.Count - 1; i >= 0; i--)
                        {
                            UniqueObjectNameCheckResponse uniqnameresp;
                            EbObject obj = ObjectCollection[i];
                            int o = 0;
                            do
                            {
                                o++;
                                uniqnameresp = objservice.Get(new UniqueObjectNameCheckRequest { ObjName = obj.Name });
                                if (!uniqnameresp.IsUnique)
                                    obj.Name = obj.Name + "(" + o + ")";
                            }
                            while (!uniqnameresp.IsUnique);

                            ObjectLifeCycleStatus _status;
                            if (request.IsDemoApp || !_isVersionedSolution)
                                _status = ObjectLifeCycleStatus.Live;
                            else
                                _status = ObjectLifeCycleStatus.Dev;

                            EbObject_Create_New_ObjectRequest ds = new EbObject_Create_New_ObjectRequest
                            {
                                Name = obj.Name,
                                DisplayName = obj.DisplayName,
                                Description = obj.Description,
                                Json = EbSerializers.Json_Serialize(obj),
                                Status = _status,
                                Relations = "_rel_obj",
                                IsSave = false,
                                Tags = "_tags",
                                Apps = appres.Id.ToString(),
                                SourceSolutionId = (obj.RefId.Split("-"))[0],
                                SourceObjId = (obj.RefId.Split("-"))[3],
                                SourceVerID = (obj.RefId.Split("-"))[4],
                                SolnId = request.SelectedSolutionId,
                                UserId = request.UserId,
                                UserAuthId = request.UserAuthId,
                                WhichConsole = request.WhichConsole,
                                IsImport = true
                            };
                            EbObject_Create_New_ObjectResponse res = objservice.Post(ds);
                            RefidMap[obj.RefId] = res.RefId;

                            // obj.ReplaceRefid(RefidMap);
                        }
                        for (int i = ObjectCollection.Count - 1; i >= 0; i--)
                        {
                            EbObject obj = ObjectCollection[i];
                            string _mapRefid = obj.RefId;
                            obj.RefId = RefidMap[obj.RefId];
                            obj.ReplaceRefid(RefidMap);
                            EbObject_SaveRequest ss = new EbObject_SaveRequest
                            {
                                RefId = RefidMap[_mapRefid],
                                Name = obj.Name,
                                DisplayName = obj.DisplayName,
                                Description = obj.Description,
                                Json = EbSerializers.Json_Serialize(obj),
                                Apps = appres.Id.ToString(),
                                SolnId = request.SelectedSolutionId,
                                UserId = request.UserId,
                                UserAuthId = request.UserAuthId,
                                WhichConsole = request.WhichConsole,
                                Relations = "_rel_obj",
                                Tags = "_tags",
                                IsImport = true
                            };
                            EbObject_SaveResponse saveRes = objservice.Post(ss);
                        }
                        Console.WriteLine("ImportApplication success");
                    }
                    else
                    {
                        Console.WriteLine("Import - ObjectCollection is null. appid: " + request.Id);
                    }
                }
                else
                {
                    Console.WriteLine(" Failed to load from appstore appid: " + request.Id);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ImportApplication" + e.Message + e.StackTrace);
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
                List<string> _refCollection = obj.DiscoverRelatedRefids();
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
        public bool IsVersioned(string selectedSolnId, int uid)
        {
            Eb_Solution soln = this.Redis.Get<Eb_Solution>(String.Format("solution_{0}", selectedSolnId));
            if (soln == null)
            {
                base.ResolveService<TenantUserServices>().Post(new UpdateSolutionObjectRequest { SolnId = selectedSolnId, UserId = uid });
                soln = this.Redis.Get<Eb_Solution>(String.Format("solution_{0}", selectedSolnId));
            }
            return soln.IsVersioningEnabled;
        }
    }
}
