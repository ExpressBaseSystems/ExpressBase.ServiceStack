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
                AppCollection = request.AppCollection,
                PackageName = request.PackageName,
                PackageDescription = request.PackageDescription,
                PackageIcon = request.PackageIcon,
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
            ExportPackage package = new ExportPackage();
            try
            {
                EbConnectionFactory ebConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
                var devservice = base.ResolveService<DevRelatedServices>();
                devservice.EbConnectionFactory = ebConnectionFactory;
                var appstoreService = base.ResolveService<AppStoreService>();
                appstoreService.EbConnectionFactory = ebConnectionFactory;

                foreach (KeyValuePair<int, string> _app in request.AppCollection)
                {
                    OrderedDictionary ObjDictionary = new OrderedDictionary();
                    AppWrapper Appwrp = devservice.Get(new GetApplicationRequest { Id = _app.Key }).AppInfo;
                    Appwrp.ObjCollection = new List<EbObject>();

                    string[] refs = _app.Value.Split(",");
                    foreach (string _refid in refs)
                        GetRelated(_refid, ObjDictionary, request.SolnId);

                    ICollection ObjectList = ObjDictionary.Values;
                    foreach (object item in ObjectList)
                        Appwrp.ObjCollection.Add(item as EbObject);
                    package.Apps.Add(Appwrp);
                }
                Log.Info("Calling FillExportData");
                package.DataSet = ExportTablesToPkg(request.SolnId);

                string packageJson = EbSerializers.Json_Serialize4AppWraper(package);
                Log.Info("Serialized packageJson. Saving to appstore");
                SaveToAppStoreResponse p = appstoreService.Post(new SaveToAppStoreRequest
                {
                    Store = new AppStore
                    {
                        Name = request.PackageName,
                        Cost = 10.00m,
                        Currency = "USD",
                        Json = packageJson,
                        Status = 1,
                        AppType = 1,
                        Description = request.PackageDescription,
                        Icon = request.PackageIcon
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

                GetOneFromAppstoreResponse response = appstoreService.Get(new GetOneFromAppStoreRequest
                {
                    Id = request.Id,
                    SolnId = request.SelectedSolutionId,
                    UserAuthId = request.UserAuthId,
                    UserId = request.UserId,
                    WhichConsole = request.WhichConsole
                });
                ExportPackage Package = response.Package;
                if (Package != null && Package.Apps != null && Package.DataSet != null)
                {
                    foreach (AppWrapper AppObj in Package.Apps)
                    {
                        List<EbObject> ObjectCollection = AppObj.ObjCollection;
                        if (ObjectCollection.Count > 0)
                        {
                            int c = 1;
                            int appId = 0;
                            string ApplicationName = response.IsPublic ? response.Title : AppObj.Name;

                            UniqueApplicationNameCheckResponse uniq_appnameresp;

                            uniq_appnameresp = devservice.Get(new UniqueApplicationNameCheckRequest { AppName = ApplicationName });
                            if (uniq_appnameresp.IsUnique)
                            {
                                CreateApplicationResponse appres = devservice.Post(new CreateApplicationRequest
                                {
                                    AppName = ApplicationName,
                                    AppType = AppObj.AppType,
                                    Description = AppObj.Description,
                                    AppIcon = AppObj.Icon,
                                    AppSettings = AppObj.AppSettings,
                                });
                                appId = appres.Id;
                            }
                            else
                            {
                                appId = uniq_appnameresp.AppId;
                            }
                            Console.WriteLine("Created application : " + ApplicationName);
                            bool _isVersionedSolution = IsVersioned(request.SelectedSolutionId, request.UserId);
                            for (int i = ObjectCollection.Count - 1; i >= 0; i--)
                            {
                                UniqueObjectNameCheckResponse uniqnameresp;
                                EbObject obj = ObjectCollection[i];
                                int o = 1;
                                string dispname = obj.DisplayName;
                                do
                                {
                                    uniqnameresp = objservice.Get(new UniqueObjectNameCheckRequest
                                    {
                                        ObjName = dispname
                                    });
                                    if (uniqnameresp.IsUnique)
                                        obj.DisplayName = dispname;
                                    else
                                        dispname = obj.DisplayName + "(" + o++ + ")";
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
                                    Apps = appId.ToString(),
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
                                    Apps = appId.ToString(),
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
                            Console.WriteLine("App & Object Creation Success.");
                        }
                        else
                        {
                            Console.WriteLine("Import - ObjectCollection is null. appid: " + request.Id);
                        }
                    }
                    ImportTablesFromPkg(Package.DataSet, request.SelectedSolutionId);

                    Console.WriteLine("ImportApplication success.");
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
            EbObject obj = null;
            EbObjectParticularVersionResponse res = (EbObjectParticularVersionResponse)objservice.Get(new EbObjectParticularVersionRequest { RefId = _refid });
            if (res.Data.Count > 0)
            {
                obj = EbSerializers.Json_Deserialize(res.Data[0].Json);
                obj.RefId = _refid;
            }
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

        public EbDataSet ExportTablesToPkg(string solnId)
        {
            EbDataSet Tables = null;
            try
            {
                string query = @"SELECT id,name FROM eb_user_types  WHERE eb_del ='F' ORDER BY id;
                            SELECT id, role_name, applicationid, description, is_anonymous FROM eb_roles WHERE eb_del ='F' ORDER BY id;
                            SELECT id, role1_id, role2_id FROM eb_role2role WHERE eb_del ='F' ORDER BY id;
                            SELECT id, type FROM eb_location_types WHERE eb_del ='F' ORDER BY id;
                            SELECT id, name, description FROM eb_usergroup WHERE eb_del ='F' ORDER BY id;";
                this.EbConnectionFactory = new EbConnectionFactory(solnId, this.Redis);
                Tables = this.EbConnectionFactory.DataDB.DoQueries(query);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in ExportInternalservice.FillExportData : " + e.Message + e.StackTrace);
            }
            return Tables;
        }
        public void ImportTablesFromPkg(EbDataSet dataSet, string solnId)
        {
            try
            {
                string query = string.Empty;
                EbDataTable T0 = dataSet.Tables[0];
                if (T0.Rows.Count > 0)
                {
                    foreach (EbDataRow dr in T0.Rows)
                    {
                        query += string.Format(@"INSERT INTO eb_user_types(id, name, eb_created_by, eb_created_at, eb_del) VALUES
                                ({0}, '{1}', 1, NOW(), 'F');", dr[0], dr[1]);
                    }
                }
                EbDataTable T1 = dataSet.Tables[1];
                if (T1.Rows.Count > 0)
                {
                    foreach (EbDataRow dr in T1.Rows)
                    {
                        query += string.Format(@"INSERT INTO eb_roles(id, role_name, applicationid, description, is_anonymous, eb_del) VALUES
                                ({0}, '{1}', {2}, '{3}', '{4}', 'F');", dr[0], dr[1], dr[2], dr[3], dr[4]);
                    }
                }
                EbDataTable T2 = dataSet.Tables[2];
                if (T2.Rows.Count > 0)
                {
                    foreach (EbDataRow dr in T2.Rows)
                    {
                        query += string.Format(@"INSERT INTO eb_role2role(id, role1_id, role2_id, createdby, createdat, eb_del) VALUES
                                ({0}, {1}, {2}, 1, NOW(), 'F');", dr[0], dr[1], dr[2]);
                    }
                }
                EbDataTable T3 = dataSet.Tables[3];
                if (T3.Rows.Count > 0)
                {
                    foreach (EbDataRow dr in T3.Rows)
                    {
                        query += string.Format(@"INSERT INTO eb_location_types(id, type, eb_created_by, eb_created_at, eb_del) VALUES
                                ({0}, '{1}', 1, NOW(), 'F');", dr[0], dr[1]);
                    }
                }
                EbDataTable T4 = dataSet.Tables[4];
                if (T4.Rows.Count > 0)
                {
                    foreach (EbDataRow dr in T4.Rows)
                    {
                        query += string.Format(@"INSERT INTO eb_usergroup(id, name, description, eb_del) VALUES
                                ({0}, '{1}', '{2}', 'F');", dr[0], dr[1], dr[2]);
                    }
                }
                this.EbConnectionFactory = new EbConnectionFactory(solnId, this.Redis);
                this.EbConnectionFactory.DataDB.DoQueries(query);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
            }
        }
    }
}
