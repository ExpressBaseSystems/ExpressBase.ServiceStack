using System.Collections.Generic;
using ServiceStack;
using ExpressBase.Data;
using System;
using ExpressBase.Objects;
using System.Data.Common;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack.Logging;
using System.Linq;
using ExpressBase.Common;
using ExpressBase.Objects.ObjectContainers;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Data;
using ExpressBase.Objects.Objects;
using Newtonsoft.Json;
using ExpressBase.Common.JsonConverters;
using ExpressBase.Objects.EmailRelated;

namespace ExpressBase.ServiceStack
{
    [ClientCanSwapTemplates]
    [DefaultView("Form")]
    [Authenticate]
    public class EbObjectService : EbBaseService
    {
        public EbObjectService(ITenantDbFactory _dbf) : base(_dbf) { }

        #region Get EbObject Queries

        // Fetch all version without json of a particular Object
        private const string Query1 = @"
SELECT 
    EOV.id, EOV.version_num, EOV.obj_changelog, EOV.commit_ts, EOV.refid, EU.firstname
FROM 
    eb_objects_ver EOV, eb_users EU
WHERE
    EOV.commit_uid = EU.id AND
    EOV.eb_objects_id=(SELECT eb_objects_id FROM eb_objects_ver WHERE refid=@refid)
ORDER BY
    EOV.id DESC";

        // Fetch particular version with json of a particular Object
        private const string Query2 = @"
SELECT
    obj_json, version_num, status
FROM
    eb_objects_ver EOV, eb_objects_status EOS
WHERE
    EOV.refid=@refid AND EOS.eb_obj_ver_id = EOV.id
ORDER BY
	EOS.id DESC 
LIMIT 1";

        // Fetch latest non-committed version with json - for EDIT
        private const string Query3 = @"
SELECT 
    EO.id, EO.obj_name, EO.obj_type, EO.obj_cur_status,EO.obj_desc,
    EOV.id,EOV.eb_objects_id,EOV.version_num, EOV.obj_changelog,EOV.commit_ts, EOV.commit_uid, EOV.obj_json, EOV.refid
FROM 
    eb_objects EO, eb_objects_ver EOV
WHERE
    EO.id = EOV.eb_objects_id AND EOV.eb_objects_id=(SELECT eb_objects_id FROM eb_objects_ver WHERE refid=@refid) AND EOV.ver_num = -1 AND EOV.commit_uid IS NULL
ORDER BY
    EO.obj_type";

        // Fetch latest committed version with json - for Execute/Run/Consume
        private const string Query4 = @"
SELECT 
    EO.id, EO.obj_name, EO.obj_type, EO.obj_cur_status, EO.obj_desc,
    EOV.id, EOV.eb_objects_id, EOV.version_num, EOV.obj_changelog, EOV.commit_ts, EOV.commit_uid, EOV.obj_json, EOV.refid
FROM 
    eb_objects EO, eb_objects_ver EOV
WHERE
    EO.id = EOV.eb_objects_id AND EOV.refid=@refid
ORDER BY
    EO.obj_type";

        // Get All latest versions of this Object Type without json
        private const string Query5 = @"
SELECT 
    EO.id, EO.obj_name, EO.obj_type, EO.obj_cur_status,EO.obj_desc,
    EOV.id, EOV.eb_objects_id, EOV.version_num, EOV.obj_changelog,EOV.commit_ts, EOV.commit_uid, EOV.refid,
    EU.firstname
FROM 
    eb_objects EO, eb_objects_ver EOV
LEFT JOIN
	eb_users EU
ON 
	EOV.commit_uid=EU.id
WHERE
    EO.id = EOV.eb_objects_id AND EO.obj_type=@type
ORDER BY
    EO.obj_name";

        private const string Query6 = @"
SELECT @function_name";

        private const string GetObjectRelations = @"
SELECT 
	id, obj_name, obj_desc 
FROM 
	eb_objects 
WHERE 
	id = ANY (SELECT eb_objects_id FROM eb_objects_ver WHERE refid IN(SELECT dependant FROM eb_objects_relations WHERE dominant=@dominant)) AND 
    obj_type=@type";

        private const string Query_AllVerList = @"
SELECT 
    EO.id, EO.obj_name, EO.obj_type, EO.obj_cur_status,EO.obj_desc,
    EOV.id, EOV.eb_objects_id, EOV.version_num, EOV.obj_changelog, EOV.commit_ts, EOV.commit_uid, EOV.refid,
    EU.firstname
FROM 
    eb_objects EO, eb_objects_ver EOV
LEFT JOIN
	eb_users EU
ON 
	EOV.commit_uid=EU.id
WHERE
    EO.id = EOV.eb_objects_id  AND EO.obj_type=@type AND COALESCE(EOV.working_mode, FALSE) <> true
ORDER BY
    EO.obj_name";

        private const string Query_ObjectList = @"SELECT 
    id, obj_name, obj_type, obj_cur_status, obj_desc  
FROM 
    eb_objects
WHERE
    obj_type=@type
ORDER BY
    obj_name";

        private const string Query_StatusHistory = @"
SELECT 
    EOS.eb_obj_ver_id, EOS.status, EU.firstname, EOS.ts, EOS.changelog, EU.profileimg   
FROM
    eb_objects_status EOS, eb_objects_ver EOV, eb_users EU
WHERE
    eb_obj_ver_id = EOV.id AND EOV.refid = @refid AND EOV.commit_uid=EU.id
ORDER BY 
EOS.id DESC";

        private const string FetchLiveversionQuery = @"
SELECT
    EO.id, EO.obj_name, EO.obj_type, EO.obj_desc,
    EOV.id, EOV.eb_objects_id, EOV.version_num, EOV.obj_changelog, EOV.commit_ts, EOV.commit_uid, EOV.obj_json, EOV.refid, EOS.status
FROM
    eb_objects_ver EOV, eb_objects_status EOS, eb_objects EO
WHERE
    EO.id = @id AND EOV.eb_objects_id = @id AND EOS.status = 3 AND EOS.eb_obj_ver_id = EOV.id";
        #endregion

       List<EbObjectWrapper> f = new List<EbObjectWrapper>();
        List<System.Data.Common.DbParameter> parameters = new List<System.Data.Common.DbParameter>();

        [CompressResponse]
        public object Get(EbObjectAllVersionsRequest request)
        {  // Fetch all version without json of a particular Object

            ILog log = LogManager.GetLogger(GetType());
            parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@refid", System.Data.DbType.String, request.RefId));
            var dt = this.TenantDbFactory.ObjectsDB.DoQuery(Query1, parameters.ToArray());

            foreach (EbDataRow dr in dt.Rows)
            {
                var _ebObject = (new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    VersionNumber = dr[1].ToString(),
                    ChangeLog = dr[2].ToString(),
                    CommitTs = Convert.ToDateTime(dr[3]),
                    RefId = dr[4].ToString(),
                    CommitUname = dr[5].ToString()
                });
                f.Add(_ebObject);
            }
            return new EbObjectAllVersionsResponse { Data = f };
        }

        [CompressResponse]
        public object Get(EbObjectParticularVersionRequest request)
        {  // Fetch particular version with json of a particular Object

            ILog log = LogManager.GetLogger(GetType());
            parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@refid", System.Data.DbType.String, request.RefId));
            var dt = this.TenantDbFactory.ObjectsDB.DoQuery(Query2, parameters.ToArray());

            foreach (EbDataRow dr in dt.Rows)
            {
                var _ebObject = (new EbObjectWrapper
                {
                    Json = dr[0].ToString(),
                    VersionNumber = dr[1].ToString(),
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), dr[2]),
                });
                f.Add(_ebObject);
            }
            return new EbObjectParticularVersionResponse { Data = f };
        }

        [CompressResponse]
        public object Get(EbObjectNonCommitedVersionRequest request)
        {
            // Fetch latest non - committed version with json - for EDIT of a particular Object

            parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@refid", System.Data.DbType.String, request.RefId));
            var dt = this.TenantDbFactory.ObjectsDB.DoQuery(Query3, parameters.ToArray());

            foreach (EbDataRow dr in dt.Rows)
            {
                var _ebObject = (new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString(),
                    EbObjectType = (EbObjectType)Convert.ToInt32(dr[2]),
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), dr[3]),
                    Description = dr[4].ToString(),
                    VersionNumber = dr[7].ToString(),
                    Json = (!string.IsNullOrEmpty(request.RefId)) ? dr[11].ToString() : null,
                    RefId = dr[12].ToString()
                });

                f.Add(_ebObject);
            }
            return new EbObjectNonCommitedVersionResponse { Data = f };
        }

        [CompressResponse]
        public object Get(EbObjectLatestCommitedRequest request)
        {
            // Fetch latest committed version with json - for Execute/Run/Consume a particular Object
            parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@refid", System.Data.DbType.String, request.RefId));
            var dt = this.TenantDbFactory.ObjectsDB.DoQuery(Query4, parameters.ToArray());

            foreach (EbDataRow dr in dt.Rows)
            {
                var _ebObject = (new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString(),
                    EbObjectType = (EbObjectType)Convert.ToInt32(dr[2]),
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), dr[4]),
                    Description = dr[5].ToString(),
                    VersionNumber = dr[8].ToString(),
                    Json = (!string.IsNullOrEmpty(request.RefId)) ? dr[12].ToString() : null,
                    RefId = dr[13].ToString()
                });

                f.Add(_ebObject);
            }
            return new EbObjectLatestCommitedResponse { Data = f };
        }

        [CompressResponse]
        public object Get(EbObjectObjListRequest request)
        { // Get All latest committed versions of this Object Type without json
            parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@type", System.Data.DbType.Int32, request.EbObjectType));
            var dt = this.TenantDbFactory.ObjectsDB.DoQuery(Query5, parameters.ToArray());

            foreach (EbDataRow dr in dt.Rows)
            {
                var _ebObject = (new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString() + dr[7].ToString(),
                    EbObjectType = (EbObjectType)Convert.ToInt32(dr[2]),
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), dr[3]),
                    Description = dr[4].ToString(),
                    VersionNumber = dr[7].ToString(),
                    CommitTs = Convert.ToDateTime(dr[9]),
                    RefId = dr[11].ToString(),
                    CommitUname = dr[12].ToString(),
                });

                f.Add(_ebObject);
            }
            return new EbObjectObjListResponse { Data = f };
        }

        [CompressResponse]
        public object Get(EbObjectListRequest request)
        { // Get All latest committed versions of this Object Type without json
            parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@type", System.Data.DbType.Int32, request.EbObjectType));
            var dt = this.TenantDbFactory.ObjectsDB.DoQuery(Query_ObjectList, parameters.ToArray());

            foreach (EbDataRow dr in dt.Rows)
            {
                var _ebObject = (new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString(),
                    EbObjectType = (EbObjectType)Convert.ToInt32(dr[2]),
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), dr[3]),
                    Description = dr[4].ToString()
                });

                f.Add(_ebObject);
            }
            return new EbObjectListResponse { Data = f };
        }


        [CompressResponse]
        public object Get(EbObjectObjLisAllVerRequest request)
        { // Get All latest committed versions of this Object Type without json
            parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@type", System.Data.DbType.Int32, request.EbObjectType));
            var dt = this.TenantDbFactory.ObjectsDB.DoQuery(Query_AllVerList, parameters.ToArray());

            Dictionary<string, List<EbObjectWrapper>> f_dict = new Dictionary<string, List<EbObjectWrapper>>();
            List<EbObjectWrapper> f_list = null;
            foreach (EbDataRow dr in dt.Rows)
            {
                string _nameKey = dr[1].ToString();
                if (!f_dict.ContainsKey(_nameKey))
                {
                    f_list = new List<EbObjectWrapper>();
                    f_dict.Add(_nameKey, f_list);
                }

                f_list.Add(new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString(),
                    EbObjectType = (EbObjectType)Convert.ToInt32(dr[2]),
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), dr[3]),
                    VersionNumber = dr[7].ToString(),
                    RefId = dr[11].ToString(),
                });
            }
            return new EbObjectObjListAllVerResponse { Data = f_dict };
        }


        [CompressResponse]
        public object Get(EbObjectRelationsRequest request)
        { //Fetch ebobjects relations

            ILog log = LogManager.GetLogger(GetType());
            parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@dominant", System.Data.DbType.String, request.DominantId));
            parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@type", System.Data.DbType.Int32, request.EbObjectType));
            var dt = this.TenantDbFactory.ObjectsDB.DoQuery(GetObjectRelations, parameters.ToArray());
            foreach (EbDataRow dr in dt.Rows)
            {
                var _ebObject = new EbObjectWrapper();

                _ebObject.Id = Convert.ToInt32(dr[0]);
                _ebObject.Name = dr[1].ToString();
                _ebObject.Description = dr[2].ToString();

                f.Add(_ebObject);
            }

            return new EbObjectRelationsResponse { Data = f };
        }

        //Get the version to open ion edit mode
        [CompressResponse]
        public object Get(EbObjectExploreObjectRequest request)
        {
            parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.Int32, request.Id));
            var dt = this.TenantDbFactory.ObjectsDB.DoQuery("SELECT * FROM public.eb_objects_exploreobject(@id)", parameters.ToArray());

            foreach (EbDataRow dr in dt.Rows)
            {
                var _ebObject = (new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString(),
                    EbObjectType = (EbObjectType)Convert.ToInt32(dr[2]),
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), dr[3]),
                    Description = dr[4].ToString(),
                    ChangeLog = dr[5].ToString(),
                    CommitTs = Convert.ToDateTime((dr[6].ToString()) == "" ? DateTime.MinValue : dr[6]),
                    CommitUname = dr[7].ToString(),
                    RefId = dr[8].ToString(),
                    VersionNumber = dr[9].ToString(),
                    WorkingMode = Convert.ToBoolean(dr[10]),
                    Json_wc = dr[12].ToString(),
                    Json_lc = dr[13].ToString(),
                    Wc_All = dr[11] as string[],
                    MajorVersionNumber = Convert.ToInt32(dr[14]),
                    MinorVersionNumber = Convert.ToInt32(dr[15]),
                    PatchVersionNumber = Convert.ToInt32(dr[16]),
                    Tags = dr[17].ToString()
                });
                f.Add(_ebObject);
            }
            return new EbObjectExploreObjectResponse { Data = f };
        }

        [CompressResponse]
        public object Get(EbObjectStatusHistoryRequest request)
        { // Get All latest committed versions of this Object Type without json
            parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@refid", System.Data.DbType.String, request.RefId));
            var dt = this.TenantDbFactory.ObjectsDB.DoQuery(Query_StatusHistory, parameters.ToArray());

            foreach (EbDataRow dr in dt.Rows)
            {
                var _ebObject = (new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), dr[1]),
                    CommitUname = dr[2].ToString(),
                    CommitTs = Convert.ToDateTime(dr[3]),
                    ChangeLog = dr[4].ToString(),
                    ProfileImage  = dr[5].ToString()
                });
                f.Add(_ebObject);
            }
            return new EbObjectStatusHistoryResponse { Data = f };
        }

        [CompressResponse]
        public object Get(EbObjectFetchLiveVersionRequest request)
        {  // Fetch particular version with json of a particular Object

            ILog log = LogManager.GetLogger(GetType());
            parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.Int32, request.Id));
            var dt = this.TenantDbFactory.ObjectsDB.DoQuery(FetchLiveversionQuery, parameters.ToArray());

            foreach (EbDataRow dr in dt.Rows)
            {
                var _ebObject = (new EbObjectWrapper
                {
                    Json = dr[0].ToString()
                });
                f.Add(_ebObject);
            }
            return new EbObjectFetchLiveVersionResponse { Data = f };
        }

        #region SaveOrCommit Queries

        

        #endregion

        // [Authenticate]
        public EbObject_CommitResponse Post(EbObject_CommitRequest request)
        {
            var obj = EbSerializers.Json_Deserialize(request.Json);
            string refId = null;
            ILog log = LogManager.GetLogger(GetType());
            log.Info("#DS insert -- entered post");

            try
            {

                using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    DbCommand cmd = null;
                    log.Info("#DS insert 1 -- con open");
                    string[] arr = { };

                    string sql = "SELECT eb_objects_commit(@id, @obj_name, @obj_desc, @obj_type, @obj_json, @obj_changelog,  @commit_uid, @src_pid, @cur_pid, @relations, @tags)";
                    cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);

                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.String, request.RefId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_name", System.Data.DbType.String, request.Name));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_type", System.Data.DbType.Int32, GetObjectType(obj)));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_desc", System.Data.DbType.String, (!string.IsNullOrEmpty(request.Description)) ? request.Description : string.Empty));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_json", NpgsqlTypes.NpgsqlDbType.Json, request.Json));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_changelog", System.Data.DbType.String, request.ChangeLog));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@commit_uid", System.Data.DbType.Int32, request.UserId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@src_pid", System.Data.DbType.String, request.TenantAccountId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@cur_pid", System.Data.DbType.String, request.TenantAccountId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@relations", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, (request.Relations != null) ? request.Relations.Split(',').Select(n => n.ToString()).ToArray() : arr));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@tags", System.Data.DbType.String, request.Tags));

                    refId = cmd.ExecuteScalar().ToString();
                }
            }
            catch (Exception e)
            {

            }
            return new EbObject_CommitResponse() { RefId = refId };
        }

        public EbObject_SaveResponse Post(EbObject_SaveRequest request)
        {
            var obj = EbSerializers.Json_Deserialize(request.Json);
            string refId = null;
            ILog log = LogManager.GetLogger(GetType());
            log.Info("#DS insert -- entered post");

            try
            {

                using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    DbCommand cmd = null;
                    log.Info("#DS insert 1 -- con open");
                    string[] arr = { };

                    string sql = "SELECT eb_objects_save(@id, @obj_name, @obj_desc, @obj_type, @obj_json, @commit_uid, @src_pid, @cur_pid, @relations, @tags)";
                    cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);

                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.String, request.RefId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_name", System.Data.DbType.String, request.Name));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_type", System.Data.DbType.Int32, GetObjectType(obj)));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_desc", System.Data.DbType.String, (!string.IsNullOrEmpty(request.Description)) ? request.Description : string.Empty));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_json", NpgsqlTypes.NpgsqlDbType.Json, request.Json));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@commit_uid", System.Data.DbType.Int32, request.UserId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@src_pid", System.Data.DbType.String, request.TenantAccountId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@cur_pid", System.Data.DbType.String, request.TenantAccountId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@relations", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, (request.Relations != null) ? request.Relations.Split(',').Select(n => n.ToString()).ToArray() : arr));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@tags", System.Data.DbType.String, request.Tags));

                    refId = cmd.ExecuteScalar().ToString();
                }
            }
            catch (Exception e)
            {

            }
            return new EbObject_SaveResponse() { RefId = refId };
        }

        public EbObject_Create_New_ObjectResponse Post(EbObject_Create_New_ObjectRequest request)
        {
            var obj = EbSerializers.Json_Deserialize(request.Json);
            string refId = null;
            ILog log = LogManager.GetLogger(GetType());
            log.Info("#DS insert -- entered post");

            try
            {
                using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    DbCommand cmd = null;
                    log.Info("#DS insert 1 -- con open");
                    string[] arr = { };

                    string sql = "SELECT eb_objects_create_new_object(@obj_name, @obj_desc, @obj_type, @obj_cur_status, @obj_json, @commit_uid, @src_pid, @cur_pid, @relations, @issave, @tags)";
                    cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);

                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_name", System.Data.DbType.String, request.Name));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_type", System.Data.DbType.Int32, GetObjectType(obj)));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_desc", System.Data.DbType.String, (!string.IsNullOrEmpty(request.Description)) ? request.Description : string.Empty));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_cur_status", System.Data.DbType.Int32, ObjectLifeCycleStatus.Dev));//request.Status
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_json", NpgsqlTypes.NpgsqlDbType.Json, request.Json));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@commit_uid", System.Data.DbType.Int32, request.UserId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@src_pid", System.Data.DbType.String, request.TenantAccountId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@cur_pid", System.Data.DbType.String, request.TenantAccountId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@relations", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, (request.Relations != null) ? request.Relations.Split(',').Select(n => n.ToString()).ToArray() : arr));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@issave", System.Data.DbType.Boolean, request.IsSave));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@tags", System.Data.DbType.String, request.Tags));

                    refId = cmd.ExecuteScalar().ToString();
                }
            }
            catch (Exception e)
            {

            }
            return new EbObject_Create_New_ObjectResponse() { RefId = refId };
        }

        public EbObject_Create_Major_VersionResponse Post(EbObject_Create_Major_VersionRequest request)
        {
            string refId = null;
            ILog log = LogManager.GetLogger(GetType());
            log.Info("#DS insert -- entered post");

            try
            {

                using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    DbCommand cmd = null;
                    log.Info("#DS insert 1 -- con open");
                    string[] arr = { };

                    string sql = "SELECT eb_objects_create_major_version(@id, @obj_type, @commit_uid, @src_pid, @cur_pid, @relations)";
                    cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);

                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.String, request.RefId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_type", System.Data.DbType.Int32, (int)request.EbObjectType));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@commit_uid", System.Data.DbType.Int32, request.UserId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@src_pid", System.Data.DbType.String, request.TenantAccountId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@cur_pid", System.Data.DbType.String, request.TenantAccountId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@relations", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, (request.Relations != null) ? request.Relations.Split(',').Select(n => n.ToString()).ToArray() : arr));

                    refId = cmd.ExecuteScalar().ToString();

                }
            }
            catch (Exception e)
            {


            }
            return new EbObject_Create_Major_VersionResponse() { RefId = refId };
        }

        public EbObject_Create_Minor_VersionResponse Post(EbObject_Create_Minor_VersionRequest request)
        {
            string refId = null;
            ILog log = LogManager.GetLogger(GetType());
            log.Info("#DS insert -- entered post");

            try
            {
                using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    DbCommand cmd = null;
                    log.Info("#DS insert 1 -- con open");
                    string[] arr = { };

                    string sql = "SELECT eb_objects_create_minor_version(@id, @obj_type, @commit_uid, @src_pid, @cur_pid, @relations)";
                    cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);

                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.String, request.RefId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_type", System.Data.DbType.Int32, (int)request.EbObjectType));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@commit_uid", System.Data.DbType.Int32, request.UserId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@src_pid", System.Data.DbType.String, request.TenantAccountId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@cur_pid", System.Data.DbType.String, request.TenantAccountId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@relations", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, (request.Relations != null) ? request.Relations.Split(',').Select(n => n.ToString()).ToArray() : arr));

                    refId = cmd.ExecuteScalar().ToString();
                }
            }
            catch (Exception e)
            {

            }
            return new EbObject_Create_Minor_VersionResponse() { RefId = refId };
        }

        public EbObject_Create_Patch_VersionResponse Post(EbObject_Create_Patch_VersionRequest request)
        {
            string refId = null;
            ILog log = LogManager.GetLogger(GetType());
            log.Info("#DS insert -- entered post");

            try
            {
                using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    DbCommand cmd = null;
                    log.Info("#DS insert 1 -- con open");
                    string[] arr = { };

                    string sql = "SELECT eb_objects_create_patch_version(@id, @obj_type, @commit_uid, @src_pid, @cur_pid, @relations)";
                    cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);

                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.String, request.RefId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_type", System.Data.DbType.Int32, (int)request.EbObjectType));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@commit_uid", System.Data.DbType.Int32, request.UserId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@src_pid", System.Data.DbType.String, request.TenantAccountId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@cur_pid", System.Data.DbType.String, request.TenantAccountId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@relations", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, (request.Relations != null) ? request.Relations.Split(',').Select(n => n.ToString()).ToArray() : arr));

                    refId = cmd.ExecuteScalar().ToString();
                }
            }
            catch (Exception e)
            {

            }
            return new EbObject_Create_Patch_VersionResponse() { RefId = refId };
        }

        public EbObjectRunSqlFunctionResponse Post(EbObjectRunSqlFunctionRequest request)
        {
            ILog log = LogManager.GetLogger(GetType());
            log.Info("#DS insert -- entered post");

            using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                DbCommand cmd = null;
                log.Info("#DS insert 1 -- con open");
                string[] arr = { };
                var code = EbSerializers.Json_Deserialize<EbSqlFunction>(request.Json).Sql;
                cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, code);
                string refId = cmd.ExecuteScalar().ToString();

                return new EbObjectRunSqlFunctionResponse() { RefId = refId };
            };
        }

        public void Post(EbObjectChangeStatusRequest request)
        {
            ILog log = LogManager.GetLogger(GetType());
            log.Info("#DS Change status");

            try
            {
                using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    DbCommand cmd = null;
                    log.Info("#DS insert 1 -- con open");

                    string sql = "SELECT eb_objects_change_status(@id, @status, @commit_uid, @obj_changelog)";
                    cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);

                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.String, request.RefId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@status", System.Data.DbType.Int32, (int) request.Status));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@commit_uid", System.Data.DbType.Int32, request.UserId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_changelog", System.Data.DbType.String, request.ChangeLog));
                    cmd.ExecuteScalar();
                }
            }
            catch (Exception e)
            {

            }
        }

        public int GetObjectType(object obj)
        {
            if (obj is EbDataSource)
                return Convert.ToInt32(EbObjectType.DataSource);
            else if (obj is EbTableVisualization)
                return Convert.ToInt32(EbObjectType.TableVisualization);
            else if (obj is EbChartVisualization)
                return Convert.ToInt32(EbObjectType.ChartVisualization);
            else if (obj is EbForm)
                return Convert.ToInt32(EbObjectType.WebForm);
            else if (obj is EbReport)
                return Convert.ToInt32(EbObjectType.Report);
            else
                return -1;
        }
    }
}