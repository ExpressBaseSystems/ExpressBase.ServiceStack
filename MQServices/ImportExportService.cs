using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.LocationNSolution;
using ExpressBase.Common.Objects;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Objects;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Security.Core;
using ExpressBase.ServiceStack.Services;
using Newtonsoft.Json;
using ServiceStack;
using ServiceStack.Messaging;
using ServiceStack.Redis;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
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
        public DevRelatedServices Devservice;

        public AppStoreService AppstoreService;

        public EbObjectService Objservice;

        public SecurityServices SecurityServices;

        public ImportExportInternalService() : base()
        {
            Devservice = base.ResolveService<DevRelatedServices>();
            AppstoreService = base.ResolveService<AppStoreService>();
            Objservice = base.ResolveService<EbObjectService>();
            SecurityServices = base.ResolveService<SecurityServices>();
        }
        public void SetConnectionFactory(string solutionId, IRedisClient redis)
        {
            EbConnectionFactory _ebConnectionFactory = new EbConnectionFactory(solutionId, redis);
            this.EbConnectionFactory = _ebConnectionFactory;
            AppstoreService.EbConnectionFactory = _ebConnectionFactory;
            Devservice.EbConnectionFactory = _ebConnectionFactory;
            Objservice.EbConnectionFactory = _ebConnectionFactory;
            SecurityServices.EbConnectionFactory = _ebConnectionFactory;
        }


        public string Post(ExportApplicationRequest request)
        {
            Log.Info("ExportApplicationRequest inside Mq");
            ExportPackage package = new ExportPackage();
            List<int> AppIdCollection = new List<int>();
            List<string> ObjectIdCollection = new List<string>();
            try
            {
                SetConnectionFactory(request.SolnId, this.Redis);

                foreach (KeyValuePair<int, List<string>> _app in request.AppCollection)
                {
                    AppIdCollection.Add(_app.Key);
                    OrderedDictionary ObjDictionary = new OrderedDictionary();
                    AppWrapper Appwrp = Devservice.Get(new GetApplicationRequest { Id = _app.Key }).AppInfo;
                    Appwrp.ObjCollection = new List<EbObject>();
                    foreach (string _refid in _app.Value)
                        GetRelated(_refid, ObjDictionary, request.SolnId);

                    ICollection ObjectList = ObjDictionary.Values;
                    foreach (object item in ObjectList)
                    {
                        Appwrp.ObjCollection.Add(item as EbObject);
                        ObjectIdCollection.Add((item as EbObject).RefId.Split("-")[3]);
                    }
                    package.Apps.Add(Appwrp);
                }

                Log.Info("Calling FillExportData");
                package.DataSet = ExportTablesToPkg(request.SolnId, ObjectIdCollection, AppIdCollection);

                string packageJson = EbSerializers.Json_Serialize4AppWraper(package);
                Log.Info("Serialized packageJson. Saving to appstore");
                SaveToAppStoreResponse p = AppstoreService.Post(new SaveToAppStoreRequest
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
                SetConnectionFactory(request.SelectedSolutionId, this.Redis);
                GetOneFromAppstoreResponse packageresponse = AppstoreService.Get(new GetOneFromAppStoreRequest
                {
                    Id = request.Id,
                    SolnId = request.SelectedSolutionId,
                    UserAuthId = request.UserAuthId,
                    UserId = request.UserId,
                    WhichConsole = request.WhichConsole
                });
                ExportPackage Package = packageresponse.Package;
                if (Package != null && Package.Apps != null)
                {
                    Dictionary<int, int> AppIdMAp = new Dictionary<int, int>();
                    Dictionary<int, KeyValuePair<int, int>> ObjectIdMAp = new Dictionary<int, KeyValuePair<int, int>>();
                    foreach (AppWrapper Application in Package.Apps)
                    {
                        if (Application.ObjCollection.Count > 0)
                        {
                            string ApplicationName = packageresponse.IsPublic ? packageresponse.Title : Application.Name;
                            int _currentAppId = CreateOrGetAppId(ApplicationName, Application);
                            if (!AppIdMAp.ContainsKey(Application.Id))
                                AppIdMAp.Add(Application.Id, _currentAppId);
                            else
                                Console.WriteLine("Duplication of appid in packge" + Application.Id + " - " + _currentAppId);

                            for (int i = Application.ObjCollection.Count - 1; i >= 0; i--)
                            {
                                EbObject obj = Application.ObjCollection[i];
                                obj.DisplayName = GetUniqDisplayName(obj.DisplayName);

                                ObjectLifeCycleStatus _status = (request.IsDemoApp || !IsVersioned(request.SelectedSolutionId, request.UserId)) ? ObjectLifeCycleStatus.Live : ObjectLifeCycleStatus.Dev;

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
                                    Apps = _currentAppId.ToString(),
                                    SourceSolutionId = (obj.RefId.Split("-"))[0],
                                    SourceObjId = (obj.RefId.Split("-"))[3],
                                    SourceVerID = (obj.RefId.Split("-"))[4],
                                    SolnId = request.SelectedSolutionId,
                                    UserId = request.UserId,
                                    UserAuthId = request.UserAuthId,
                                    WhichConsole = request.WhichConsole,
                                    IsImport = true
                                };
                                EbObject_Create_New_ObjectResponse res = Objservice.Post(ds);
                                RefidMap[obj.RefId] = res.RefId;
                                ObjectIdMAp.Add(Convert.ToInt32(obj.RefId.Split("-")[3]), new KeyValuePair<int, int>(Convert.ToInt32(res.RefId.Split("-")[3]), Convert.ToInt32(res.RefId.Split("-")[2])));
                            }

                            //Updating Refid
                            for (int i = Application.ObjCollection.Count - 1; i >= 0; i--)
                            {
                                EbObject obj = Application.ObjCollection[i];
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
                                    Apps = _currentAppId.ToString(),
                                    SolnId = request.SelectedSolutionId,
                                    UserId = request.UserId,
                                    UserAuthId = request.UserAuthId,
                                    WhichConsole = request.WhichConsole,
                                    Relations = "_rel_obj",
                                    Tags = "_tags",
                                    IsImport = true
                                };
                                EbObject_SaveResponse saveRes = Objservice.Post(ss);
                            }
                            Console.WriteLine("App & Object Creation Success.");
                        }
                        else
                        {
                            Console.WriteLine("Import - ObjectCollection is null. appid: " + request.Id);
                        }
                    }

                    try
                    {
                        ImportFullTablesFromPkg(Package.DataSet.FullExportTables, request.SelectedSolutionId);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Error at Import.ImportFullTablesFromPkg " + e.Message + e.StackTrace);
                    }
                    try
                    {
                        ImportConditionalTablesFromPkg(Package.DataSet.ConditionalExportTables, request.SelectedSolutionId, AppIdMAp, ObjectIdMAp, request.UserId);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Error at Import.ImportConditionalTablesFromPkg " + e.Message + e.StackTrace);
                    }
                    try
                    {
                        UpdateSequencetoMax();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Error at Import.UpdateSequencetoMax " + e.Message + e.StackTrace);
                    }
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

        public PackageDataSets ExportTablesToPkg(string solnId, List<string> ObjectIdCollection, List<int> AppIdCollection)
        {
            PackageDataSets Sets = new PackageDataSets();
            try
            {
                string fullexport = @"SELECT id,name FROM 
                                eb_user_types  
                            WHERE eb_del ='F' ORDER BY id; 

                            SELECT id, type FROM
                                eb_location_types 
                            WHERE eb_del ='F' ORDER BY id; 

                            SELECT id, name, description FROM
                                eb_usergroup 
                            WHERE eb_del ='F' ORDER BY id;";
                Sets.FullExportTables = this.EbConnectionFactory.DataDB.DoQueries(fullexport);

                string conditionalexport = string.Format(@"SELECT id, role_name, applicationid, description, is_anonymous FROM
                                            eb_roles
                                        WHERE eb_del = 'F' AND applicationid IN({0})
                                        AND id IN (SELECT distinct role_id FROM eb_role2permission WHERE eb_del ='F'  AND obj_id IN ({1}))
                                        ORDER BY applicationid;", string.Join<int>(",", AppIdCollection), string.Join(",", ObjectIdCollection));

                conditionalexport += string.Format(@"SELECT id, role1_id, role2_id FROM
                                            eb_role2role 
                                        WHERE eb_del ='F'
                                        AND role1_id IN (SELECT id FROM  eb_roles WHERE eb_del ='F' AND applicationid IN({0}) AND id IN (SELECT distinct role_id FROM eb_role2permission WHERE eb_del ='F'  AND obj_id IN ({1})))
                                        ORDER BY id;", string.Join<int>(",", AppIdCollection), string.Join(",", ObjectIdCollection));

                conditionalexport += string.Format(@"SELECT id, role_id, obj_id, op_id FROM
                                            eb_role2permission 
                                        WHERE eb_del ='F'
                                        AND role_id IN (SELECT id FROM  eb_roles WHERE eb_del ='F' AND applicationid IN({0}))
                                        AND obj_id IN ({1})
                                        ORDER BY id;", string.Join<int>(",", AppIdCollection), string.Join(",", ObjectIdCollection));

                Sets.ConditionalExportTables = this.EbConnectionFactory.DataDB.DoQueries(conditionalexport);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in ExportInternalservice.FillExportData : " + e.Message + e.StackTrace);
            }
            return Sets;
        }

        public void ImportFullTablesFromPkg(EbDataSet dataSet, string solnId)
        {
            try
            {
                if (dataSet != null)
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
                            query += string.Format(@"INSERT INTO eb_location_types(id, type, eb_created_by, eb_created_at, eb_del) VALUES
                                ({0}, '{1}', 1, NOW(), 'F');", dr[0], dr[1]);
                        }
                    }
                    EbDataTable T2 = dataSet.Tables[2];
                    if (T2.Rows.Count > 0)
                    {
                        foreach (EbDataRow dr in T2.Rows)
                        {
                            query += string.Format(@"INSERT INTO eb_usergroup(id, name, description, eb_del) VALUES
                                ({0}, '{1}', '{2}', 'F');", dr[0], dr[1], dr[2]);
                        }
                    }
                    this.EbConnectionFactory = new EbConnectionFactory(solnId, this.Redis);
                    this.EbConnectionFactory.DataDB.DoQueries(query);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
            }
        }

        public void ImportConditionalTablesFromPkg(EbDataSet dataSet, string solnId, Dictionary<int, int> AppIdMap, Dictionary<int, KeyValuePair<int, int>> ObjectIdMap, int userid)
        {
            Dictionary<int, ExportRole> RolesCollection = new Dictionary<int, ExportRole>();
            EbDataTable T0 = dataSet.Tables[0];
            if (T0.Rows.Count > 0)
            {
                foreach (EbDataRow dr in T0.Rows)
                {
                    int appid = Convert.ToInt32(dr[2]);
                    int oldRoleid = Convert.ToInt32(dr[0]);
                    RolesCollection.Add(oldRoleid, new ExportRole
                    {
                        AppId = AppIdMap[appid],
                        Role = new EbRole { Id = Convert.ToInt32(dr[0]), Name = dr[1].ToString(), Description = dr[3].ToString(), },
                        IsAnonymous = dr[4].ToString(),
                        Permissions = new List<EbPermissions>(),
                        LocationIds = new List<int> { { 1 } }
                    });
                }
            }
            EbDataTable T1 = dataSet.Tables[1];
            if (T1.Rows.Count > 0)
            {
                foreach (EbDataRow dr in T1.Rows)
                {
                    int oldRoleid = Convert.ToInt32(dr[1]);
                    int roleid2 = Convert.ToInt32(dr[2]);
                    if (roleid2 > 0)
                    {
                        if (RolesCollection[oldRoleid].DependantRoles == null)
                            RolesCollection[oldRoleid].DependantRoles = new List<int>();
                        RolesCollection[oldRoleid].DependantRoles.Add(roleid2);
                    }
                }
            }
            EbDataTable T2 = dataSet.Tables[2];
            if (T2.Rows.Count > 0)
            {
                foreach (EbDataRow dr in T2.Rows)
                {
                    int oldRoleid = Convert.ToInt32(dr[1]);
                    KeyValuePair<int, int> pair = ObjectIdMap[Convert.ToInt32(dr[2])];//<objectid,objtype>
                    RolesCollection[oldRoleid].Permissions.Add(new EbPermissions(pair.Value /*type*/, pair.Key, Convert.ToInt32(dr[3])));
                }
            }

            foreach (KeyValuePair<int, ExportRole> role in RolesCollection)
            {
                ExportRole exp_role = role.Value;
                string query = string.Format(@"INSERT INTO eb_roles(id, role_name) VALUES
                                ({0},'{1}');", exp_role.Role.Id, exp_role.Role.Name);
                int c = this.EbConnectionFactory.DataDB.DoNonQuery(query);

                if (c > 0)
                {
                    Dictionary<string, object> Dict = new Dictionary<string, object>();
                    Dict["roleid"] = exp_role.Role.Id;
                    Dict["applicationid"] = exp_role.AppId;
                    Dict["role_name"] = exp_role.Role.Name;
                    Dict["Description"] = exp_role.Role.Description;
                    Dict["IsAnonymous"] = exp_role.IsAnonymous;
                    Dict["users"] = string.Empty;
                    Dict["permission"] = string.Empty;
                    if (exp_role.Permissions.Count > 0)
                    {
                        List<string> permissionString = new List<string>();
                        foreach (EbPermissions permission in exp_role.Permissions)
                        {
                            permissionString.Add(exp_role.AppId.ToString().PadLeft(3, '0') + "-" + /*this is obj type*/permission._id.ToString().PadLeft(2, '0') + "-" + permission._object_id.ToString().PadLeft(5, '0') + "-" + permission._operation_id.ToString().PadLeft(2, '0'));
                        }
                        Dict["permission"] = string.Join(',', permissionString);
                    }
                    if (exp_role.DependantRoles != null)
                    {

                    }
                    Dict["dependants"] = (exp_role.DependantRoles == null) ? string.Empty : string.Join<int>(",", exp_role.DependantRoles);
                    Dict["locations"] = (exp_role.LocationIds == null) ? string.Empty : string.Join<int>(",", exp_role.LocationIds);

                    SaveRoleResponse res = (SaveRoleResponse)SecurityServices.Post(new SaveRoleRequest { Colvalues = Dict, UserId = userid, SolnId = solnId });//if res.id  = 0 success
                }
            }
        }

        public void UpdateSequencetoMax()
        {
            string sequenceUpdateQ = @"SELECT setval('eb_roles_id_seq', (SELECT MAX(id) FROM eb_roles)+1);
                                           SELECT setval('eb_location_types_id_seq', (SELECT MAX(id) FROM eb_location_types)+1);
                                           SELECT setval('eb_user_types_id_seq', (SELECT MAX(id) FROM eb_user_types)+1);
                                           SELECT setval('eb_usergroup_id_seq', (SELECT MAX(id) FROM eb_usergroup)+1);";
            this.EbConnectionFactory.DataDB.DoQueries(sequenceUpdateQ);
        }

        public int CreateOrGetAppId(string ApplicationName, AppWrapper AppObj)
        {
            int _currentAppId = 0;
            try
            {
                UniqueApplicationNameCheckResponse uniq_appnameresp = Devservice.Get(new UniqueApplicationNameCheckRequest { AppName = ApplicationName });
                if (uniq_appnameresp.IsUnique)
                {
                    CreateApplicationResponse appres = Devservice.Post(new CreateApplicationRequest
                    {
                        AppName = ApplicationName,
                        AppType = AppObj.AppType,
                        Description = AppObj.Description,
                        AppIcon = AppObj.Icon,
                        AppSettings = AppObj.AppSettings,
                    });
                    _currentAppId = appres.Id;
                }
                else
                    _currentAppId = uniq_appnameresp.AppId;

                Console.WriteLine("Import.CreateOrGetAppId success : " + ApplicationName);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
            }
            return _currentAppId;
        }

        public string GetUniqDisplayName(string name)
        {
            UniqueObjectNameCheckResponse uniqnameresp;

            int o = 1;
            string dispname = name;
            do
            {

                uniqnameresp = Objservice.Get(new UniqueObjectNameCheckRequest
                {
                    ObjName = dispname
                });
                if (!uniqnameresp.IsUnique)
                    dispname = name + "(" + o++ + ")";
            }
            while (!uniqnameresp.IsUnique);
            return dispname;
        }
    }
    public class ExportRole
    {
        public EbRole Role { get; set; }

        public int AppId { get; set; }

        public string IsAnonymous { get; set; }

        public List<int> LocationIds { get; set; }

        public List<EbPermissions> Permissions { get; set; }

        public List<int> DependantRoles { get; set; }


    }
}
