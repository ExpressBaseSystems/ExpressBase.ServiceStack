using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Microsoft.Rest;
using ServiceStack;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class ImportExportService : EbBaseService
    {
        public ImportExportService(IEbConnectionFactory _dbf) : base(_dbf) { }

        public GetOneFromAppstoreResponse Get(GetOneFromAppStoreRequest request)
        {
            DbParameter[] Parameters = { InfraConnectionFactory.ObjectsDB.GetNewParameter(":id", EbDbTypes.Int32, request.Id) };
            EbDataTable dt = InfraConnectionFactory.ObjectsDB.DoQuery("SELECT * FROM eb_appstore WHERE id = :id", Parameters);
            return new GetOneFromAppstoreResponse
            {
                Wrapper = (AppWrapper)EbSerializers.Json_Deserialize(dt.Rows[0][7].ToString())
            };
        }

        public GetAllFromAppstoreResponse Get(GetAllFromAppStoreRequest request)
        {
            List<AppStore> _storeCollection = new List<AppStore>();
            EbDataTable dt = InfraConnectionFactory.ObjectsDB.DoQuery(string.Format(@"
                                            SELECT EAS.id, EAS.app_name, EAS.status, EAS.user_solution_id, EAS.cost, EAS.created_by, EAS.created_at, EAS.json,
                                                EAS.currency, EAS.eb_del, EAS.app_type, EAS.description, EAS.icon, 
                                                ES.solution_name, EU.fullname
                                                FROM eb_appstore EAS, eb_solutions ES, eb_users EU
                                            WHERE(( EAS.user_solution_id = '{0}' AND EAS.status=1) OR EAS.status=2)
                                                AND EAS.eb_del='F' AND
											EAS.user_solution_id = ES.esolution_id AND 
											ES.tenant_id = EU.id ;", request.TenantAccountId));
            foreach (EbDataRow _row in dt.Rows)
            {
                AppStore _app = new AppStore
                {
                    Id = Convert.ToInt32(_row[0]),
                    Name = _row[1].ToString(),
                    Status = Convert.ToInt32(_row[2]),
                    SolutionId= _row[3].ToString(),
                    Cost = Convert.ToInt32(_row[4]),
                    CreatedBy = Convert.ToInt32(_row[5]),
                    CreatedAt = Convert.ToDateTime(_row[6]),
                    Json = _row[7].ToString(),
                    Currency = _row[8].ToString(),
                    AppType = Convert.ToInt32(_row[10]),
                    Description = _row[11].ToString(),
                    Icon = _row[12].ToString(),
                    SolutionName = _row[13].ToString(),
                    TenantName = _row[14].ToString()
                };
                _storeCollection.Add(_app);
            }
            return new GetAllFromAppstoreResponse { Apps = _storeCollection };
        }

        public SaveToAppStoreResponse Post(SaveToAppStoreRequest request)
        {
            using (DbConnection con = this.InfraConnectionFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string sql = @"INSERT INTO eb_appstore (app_name, status, user_solution_id, cost, created_by, created_at, json, currency, app_type, description, icon)
                                                VALUES (:app_name, :status, :user_solution_id, :cost, :created_by, Now(), :json, :currency, :app_type, :description, :icon);";
                DbCommand cmd = InfraConnectionFactory.ObjectsDB.GetNewCommand(con, sql);
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":app_name", EbDbTypes.String, request.Store.Name));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":status", EbDbTypes.Int32, request.Store.Status));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":user_solution_id", EbDbTypes.String, request.TenantAccountId));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":cost", EbDbTypes.Int32, request.Store.Cost));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":created_by", EbDbTypes.Int32, request.UserId));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":json", EbDbTypes.Json, request.Store.Json));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":currency", EbDbTypes.String, request.Store.Currency));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":app_type", EbDbTypes.Int32, request.Store.AppType));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":description", EbDbTypes.String, request.Store.Description));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":icon", EbDbTypes.String, request.Store.Icon));
                object x = cmd.ExecuteScalar();
                return new SaveToAppStoreResponse { };
            }
        }

        public ExportApplicationResponse Post(ExportApplicationRequest request)
        {
            string result = "Success";
            int app_id = 1;
            OrderedDictionary ObjDictionary = new OrderedDictionary();
            try
            {
                GetApplicationResponse appRes = base.ResolveService<DevRelatedServices>().Get(new GetApplicationRequest { Id = app_id });
                AppWrapper AppObj = appRes.AppInfo;
                AppObj.ObjCollection = new List<EbObject>();
                string[] refs = request.Refids.Split(",");
                foreach (string _refid in refs)
                    GetRelated(_refid, ObjDictionary);

                ICollection ObjectList = ObjDictionary.Values;
                foreach (object item in ObjectList)
                    AppObj.ObjCollection.Add(item as EbObject);

                string stream = EbSerializers.Json_Serialize(AppObj);
                SaveToAppStoreResponse p = Post(new SaveToAppStoreRequest
                {
                    Store = new AppStore
                    {
                        Name = AppObj.Name,
                        Cost = 1000,
                        Currency = "USD",
                        Json = stream,
                        Status = 1,
                        AppType = 1,
                        Description = AppObj.Description,
                        Icon = AppObj.Icon
                    },
                    TenantAccountId = request.TenantAccountId,
                    UserId = request.UserId,
                    UserAuthId = request.UserAuthId,
                    WhichConsole = request.WhichConsole
                });
            }
            catch (Exception e)
            {
                result = "Failed";
                Console.WriteLine(e.Message);
            }

            return new ExportApplicationResponse { Result = result };
        }

        public ImportApplicationResponse Get(ImportApplicationRequest request)
        {
            string result = "Success";
            Dictionary<string, string> RefidMap = new Dictionary<string, string>();
            try
            {
                GetOneFromAppstoreResponse resp = base.ResolveService<ImportExportService>().Get(new GetOneFromAppStoreRequest { Id = request.Id });
                AppWrapper AppObj = resp.Wrapper;
                List<EbObject> ObjectCollection = AppObj.ObjCollection;
                UniqueApplicationNameCheckResponse uniq_appnameresp;

                do
                {
                    uniq_appnameresp = base.ResolveService<DevRelatedServices>().Get(new UniqueApplicationNameCheckRequest { AppName = AppObj.Name });
                    if (!uniq_appnameresp.IsUnique)
                        AppObj.Name = AppObj.Name + "(1)";
                }
                while (!uniq_appnameresp.IsUnique);
                CreateApplicationResponse appres = base.ResolveService<DevRelatedServices>().Post(new CreateApplicationDevRequest
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
                        uniqnameresp = base.ResolveService<EbObjectService>().Get(new UniqueObjectNameCheckRequest { ObjName = obj.Name });
                        if (!uniqnameresp.IsUnique)
                            obj.Name = obj.Name + "(1)";
                    }
                    while (!uniqnameresp.IsUnique);

                    obj.ReplaceRefid(RefidMap);
                    EbObject_Create_New_ObjectRequest ds = new EbObject_Create_New_ObjectRequest
                    {
                        Name = obj.Name,
                        Description = obj.Description,
                        Json = EbSerializers.Json_Serialize(obj),
                        Status = ObjectLifeCycleStatus.Dev,
                        Relations = "_rel_obj",
                        IsSave = false,
                        Tags = "_tags",
                        Apps = appres.id.ToString(),
                        SourceSolutionId = (obj.RefId.Split("-"))[0],
                        SourceObjId = (obj.RefId.Split("-"))[3],
                        SourceVerID = (obj.RefId.Split("-"))[4],
                        TenantAccountId = request.TenantAccountId,
                        UserId = request.UserId,
                        UserAuthId = request.UserAuthId,
                        WhichConsole = request.WhichConsole
                    };
                    EbObject_Create_New_ObjectResponse res = base.ResolveService<EbObjectService>().Post(ds);
                    RefidMap[obj.RefId] = res.RefId;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                result = "Failed";
            }
            return new ImportApplicationResponse { Result = result };
        }

        public ShareToPublicResponse Post(ShareToPublicRequest request)
        {
            using (DbConnection con = this.InfraConnectionFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string sql = @"
                        INSERT INTO eb_appstore_detailed(app_store_id, title, is_free, published_at, published_by,
								 short_desc, tags, detailed_desc, demo_links, video_links, images, pricing_desc)
                            VALUES (:app_store_id, :title, :is_free, Now(), :published_by, :short_desc, :tags,
		                            :detailed_desc, :demo_links, :video_links, :images, :pricing_desc);

                        UPDATE eb_appstore SET status = 2, cost = :cost where id = :app_store_id;";
                DbCommand cmd = InfraConnectionFactory.ObjectsDB.GetNewCommand(con, sql);
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":app_store_id", EbDbTypes.Int32, request.AppStoreId));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":title", EbDbTypes.String, request.Title));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":is_free", EbDbTypes.String, (Convert.ToInt32(request.IsFree) == 1) ? "T" : "F"));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":published_by", EbDbTypes.Int32, request.UserId));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":short_desc", EbDbTypes.String, request.ShortDesc));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":tags", EbDbTypes.String, request.Tags));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":detailed_desc", EbDbTypes.String, request.DetailedDesc));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":demo_links", EbDbTypes.String, request.DemoLinks));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":video_links", EbDbTypes.String, request.VideoLinks));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":images", EbDbTypes.String, request.Images));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":pricing_desc", EbDbTypes.String, request.PricingDesc));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":cost", EbDbTypes.Int32, request.Cost));
                var p = cmd.ExecuteNonQuery();
            }
            return new ShareToPublicResponse { };
        }
        public void GetRelated(string _refid, OrderedDictionary ObjDictionary)
        {
            EbObject obj = null;

            if (ObjDictionary.Contains(_refid))
            {
                obj = (EbObject)ObjDictionary[_refid];
                ObjDictionary.Remove(_refid);
            }
            else
                obj = GetObjfromDB(_refid);

            ObjDictionary.Add(_refid, obj);

            string RefidS = obj.DiscoverRelatedRefids();

            string[] _refCollection = RefidS.Split(",");
            foreach (string _ref in _refCollection)
            {
                if (_ref.Trim() != string.Empty)
                {
                    GetRelated(_ref, ObjDictionary);
                    Console.WriteLine(_ref);
                    Console.WriteLine(_ref);
                }
            }
        }

        public EbObject GetObjfromDB(string _refid)
        {
            EbObjectService ObjectService = base.ResolveService<EbObjectService>();
            EbObjectParticularVersionResponse res = (EbObjectParticularVersionResponse)ObjectService.Get(new EbObjectParticularVersionRequest { RefId = _refid });
            EbObject obj = EbSerializers.Json_Deserialize(res.Data[0].Json);
            obj.RefId = _refid;
            return obj;
        }
    }
}
