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
    EOV.id, EOV.ver_num, EOV.obj_changelog, EOV.commit_ts, EOV.refid, EU.firstname
FROM 
    eb_objects_ver EOV, eb_users EU
WHERE
    EOV.commit_uid = EU.id AND
    EOV.eb_objects_id=(SELECT eb_objects_id FROM eb_objects_ver WHERE refid=@refid)
ORDER BY
    ver_num DESC";

        // Fetch particular version with json of a particular Object
        private const string Query2 = "SELECT obj_json FROM eb_objects_ver WHERE refid=@refid";

        // Fetch latest non-committed version with json - for EDIT
        private const string Query3 = @"
SELECT 
    EO.id, EO.obj_name, EO.obj_type, EO.obj_last_ver_id, EO.obj_cur_status,EO.obj_desc,
    EOV.id,EOV.eb_objects_id,EOV.ver_num, EOV.obj_changelog,EOV.commit_ts, EOV.commit_uid, EOV.obj_json, EOV.refid
FROM 
    eb_objects EO, eb_objects_ver EOV
WHERE
    EO.id = EOV.eb_objects_id AND EOV.eb_objects_id=(SELECT eb_objects_id FROM eb_objects_ver WHERE refid=@refid) AND EOV.ver_num = -1 AND EOV.commit_uid IS NULL
ORDER BY
    EO.obj_type";

        // Fetch latest committed version with json - for Execute/Run/Consume
        private const string Query4 = @"
SELECT 
    EO.id, EO.obj_name, EO.obj_type, EO.obj_last_ver_id, EO.obj_cur_status,EO.obj_desc,
    EOV.id,EOV.eb_objects_id,EOV.ver_num, EOV.obj_changelog,EOV.commit_ts, EOV.commit_uid, EOV.obj_json, EOV.refid
FROM 
    eb_objects EO, eb_objects_ver EOV
WHERE
    EO.id = EOV.eb_objects_id AND EOV.refid=@refid
ORDER BY
    EO.obj_type";

        // Get All latest versions of this Object Type without json
        private const string Query5 = @"
SELECT 
    EO.id, EO.obj_name, EO.obj_type, EO.obj_last_ver_id, EO.obj_cur_status,EO.obj_desc,
    EOV.id, EOV.eb_objects_id, EOV.ver_num, EOV.obj_changelog,EOV.commit_ts, EOV.commit_uid, EOV.refid,
    EU.firstname
FROM 
    eb_objects EO, eb_objects_ver EOV
LEFT JOIN
	eb_users EU
ON 
	EOV.commit_uid=EU.id
WHERE
    EO.id = EOV.eb_objects_id AND EOV.ver_num=EO.obj_last_ver_id AND EO.obj_type=@type
ORDER BY
    EO.obj_name";

        private const string Query6 = @"
SELECT @function_name";

        private const string Query_AllVerList = @"
SELECT 
    EO.id, EO.obj_name, EO.obj_type, EO.obj_last_ver_id, EO.obj_cur_status,EO.obj_desc,
    EOV.id, EOV.eb_objects_id, EOV.ver_num, EOV.obj_changelog,EOV.commit_ts, EOV.commit_uid, EOV.refid,
    EU.firstname
FROM 
    eb_objects EO, eb_objects_ver EOV
LEFT JOIN
	eb_users EU
ON 
	EOV.commit_uid=EU.id
WHERE
    EO.id = EOV.eb_objects_id  AND EO.obj_type=@type
ORDER BY
    EO.obj_name";
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
                    VersionNumber = Convert.ToInt32(dr[1]),
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
                    Json = dr[0].ToString()
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
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), dr[4]),
                    Description = dr[5].ToString(),
                    VersionNumber = Convert.ToInt32(dr[3]),
                    Json = (!string.IsNullOrEmpty(request.RefId)) ? dr[12].ToString() : null,
                    RefId = dr[13].ToString()
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
                    VersionNumber = Convert.ToInt32(dr[8]),
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
                    Name = dr[1].ToString(),
                    EbObjectType = (EbObjectType)Convert.ToInt32(dr[2]),
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), dr[4]),
                    Description = dr[5].ToString(),
                    VersionNumber = Convert.ToInt32(dr[8]),
                    CommitTs = Convert.ToDateTime(dr[10]),
                    RefId = dr[12].ToString(),
                    CommitUname = dr[13].ToString(),
                });

                f.Add(_ebObject);
            }
            return new EbObjectObjListResponse { Data = f };
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
                    EbObjectType= (EbObjectType)Convert.ToInt32(dr[2]),
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus),dr[4]),
                    VersionNumber = Convert.ToInt32(dr[8]),
                    RefId = dr[12].ToString(),
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
        [CompressResponse]


        #region SaveOrCommit Queries

        private const string Query_FirstCommit = @"
INSERT INTO eb_objects 
    (obj_name, obj_desc, obj_type, obj_last_ver_id, obj_cur_status) 
VALUES
    (@obj_name, @obj_desc, @obj_type, 1, @obj_cur_status)  RETURNING id;

INSERT INTO eb_objects_ver
    (eb_objects_id, ver_num, obj_json, commit_uid, commit_ts) 
VALUES
    (CURRVAL('eb_objects_id_seq'), 1, @obj_json, @commit_uid, NOW());

INSERT INTO eb_objects_ver
    (eb_objects_id, ver_num, obj_json) 
VALUES
    (CURRVAL('eb_objects_id_seq'), -1, @obj_json);

INSERT INTO eb_objects_relations
    (dominant,dependant)
VALUES
    (UNNEST(@relations),CURRVAL('eb_objects_id_seq'))
";


        private const string Query_FirstCommit_without_rel = @"
INSERT INTO eb_objects 
    (obj_name, obj_desc, obj_type, obj_last_ver_id, obj_cur_status) 
VALUES
    (@obj_name, @obj_desc, @obj_type, 1, @obj_cur_status)  RETURNING id;

INSERT INTO eb_objects_ver
    (eb_objects_id, ver_num, obj_json, commit_uid, commit_ts ) 
VALUES
    (CURRVAL('eb_objects_id_seq'), 1, @obj_json, @commit_uid, NOW() );

INSERT INTO eb_objects_ver
    (eb_objects_id, ver_num, obj_json ) 
VALUES
    (CURRVAL('eb_objects_id_seq'), -1, @obj_json );
";

        private const string Query_SubsequentCommit = @"
UPDATE eb_objects 
SET 
    obj_name=@obj_name, obj_desc=@obj_desc, 
    obj_last_ver_id=(SELECT MAX(ver_num)+1 FROM eb_objects_ver WHERE eb_objects_id=@id), 
    obj_cur_status=@obj_cur_status 
WHERE 
    id=@id RETURNING id; 

UPDATE eb_objects_ver
SET
    obj_json=@obj_json, obj_changelog=@obj_changelog, ver_num=(SELECT MAX(ver_num)+1 FROM eb_objects_ver WHERE eb_objects_id=@id), commit_uid=@commit_uid, commit_ts=NOW()
WHERE
    eb_objects_id=@id AND commit_uid IS NULL AND ver_num=-1;

INSERT INTO eb_objects_ver
    (eb_objects_id, ver_num, obj_json) 
VALUES
    (@id, -1, @obj_json)";

        private const string Query_Save = @"
UPDATE eb_objects SET obj_name=@obj_name, obj_desc=@obj_desc WHERE id=@id;
UPDATE eb_objects_ver SET obj_json=@obj_json WHERE eb_objects_id=@id AND commit_uid IS NULL AND ver_num=-1;";


        private const string GetObjectRelations = @"
SELECT 
	id, obj_name, obj_desc 
FROM 
	eb_objects 
WHERE 
	id = ANY (SELECT eb_objects_id FROM eb_objects_ver WHERE refid IN(SELECT dependant FROM eb_objects_relations WHERE dominant=@dominant)) AND 
    obj_type=@type";

        #endregion

        // [Authenticate]
        public EbObjectFirstCommitResponse Post(EbObjectFirstCommitRequest request)
        {
            string refId = null;
            ILog log = LogManager.GetLogger(GetType());
            log.Info("#DS insert -- entered post");

            EbObject _eb_object = null;
            try
            {
                if (request.EbObjectType == (int)EbObjectType.DataVisualization)
                    _eb_object = EbSerializers.Json_Deserialize<EbDataVisualization>(request.Json);
                else if (request.EbObjectType == (int)EbObjectType.DataSource)
                    _eb_object = EbSerializers.Json_Deserialize<EbDataSource>(request.Json);
                else if (request.EbObjectType == (int)EbObjectType.FilterDialog)
                    _eb_object = EbSerializers.Json_Deserialize<EbFilterDialog>(request.Json);
                else if (request.EbObjectType == (int)EbObjectType.WebForm)
                    _eb_object = EbSerializers.Json_Deserialize<EbForm>(request.Json);
                else if (request.EbObjectType == (int)EbObjectType.EmailBuilder)
                    _eb_object = EbSerializers.Json_Deserialize<EbEmailBuilder>(request.Json);

                if (_eb_object != null)
                {
                    using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
                    {
                        con.Open();
                        DbCommand cmd = null;
                        log.Info("#DS insert 1 -- con open");
                        string[] arr = { };

                        string sql = "SELECT eb_objects_first_commit(@obj_name, @obj_desc, @obj_type, @obj_cur_status, @obj_json, @commit_uid, @src_pid, @cur_pid, @relations);";
                        cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);

                        cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_name", System.Data.DbType.String, request.Name));
                        cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_desc", System.Data.DbType.String, (!string.IsNullOrEmpty(request.Description)) ? request.Description : string.Empty));
                        cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_type", System.Data.DbType.Int32, (int)request.EbObjectType));
                        cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_cur_status", System.Data.DbType.Int32, ObjectLifeCycleStatus.Development));
                        cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_json", NpgsqlTypes.NpgsqlDbType.Json, request.Json));
                        cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@commit_uid", System.Data.DbType.Int32, request.UserId));
                        cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@src_pid", System.Data.DbType.String, request.TenantAccountId));
                        cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@cur_pid", System.Data.DbType.String, request.TenantAccountId));
                        cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@relations", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, (request.Relations != null) ? request.Relations.Split(',').Select(n => n.ToString()).ToArray() : arr));

                        refId = cmd.ExecuteScalar().ToString();

                        if (request.EbObjectType == (int)EbObjectType.DataVisualization)
                            this.Redis.Set<EbDataVisualization>(refId, _eb_object as EbDataVisualization);
                        if (request.EbObjectType == (int)EbObjectType.DataSource)
                            this.Redis.Set<EbDataSource>(refId, _eb_object as EbDataSource);
                        if (request.EbObjectType == (int)EbObjectType.FilterDialog)
                            this.Redis.Set<EbFilterDialog>(refId, _eb_object as EbFilterDialog);
                    }
                }
            }
            catch (Exception e)
            {

            }

            return new EbObjectFirstCommitResponse() { RefId = refId };
        }

        public EbObjectSubsequentCommitResponse Post(EbObjectSubsequentCommitRequest request)
        {
            string refId = null;
            ILog log = LogManager.GetLogger(GetType());
            log.Info("#DS insert -- entered post");

            EbObject _eb_object = null;
            try
            {
                if (request.EbObjectType == (int)EbObjectType.DataVisualization)
                    _eb_object = EbSerializers.Json_Deserialize<EbDataVisualization>(request.Json);
                if (request.EbObjectType == (int)EbObjectType.DataSource)
                    _eb_object = EbSerializers.Json_Deserialize<EbDataSource>(request.Json);
                if (request.EbObjectType == (int)EbObjectType.FilterDialog)
                    _eb_object = EbSerializers.Json_Deserialize<EbFilterDialog>(request.Json);


                using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    DbCommand cmd = null;
                    log.Info("#DS insert 1 -- con open");
                    string[] arr = { };

                    string sql = "SELECT eb_objects_subsequentcommit_save2(@id, @obj_name, @obj_desc, @obj_type, @obj_cur_status, @obj_json, @obj_changelog, @issave, @commit_uid, @src_pid, @cur_pid, @relations)";
                    cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);

                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.String, request.RefId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_name", System.Data.DbType.String, _eb_object.Name));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_type", System.Data.DbType.Int32, (int)request.EbObjectType));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_desc", System.Data.DbType.String, (!string.IsNullOrEmpty(request.Description)) ? request.Description : string.Empty));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_cur_status", System.Data.DbType.Int32, ObjectLifeCycleStatus.Development));//request.Status
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_json", NpgsqlTypes.NpgsqlDbType.Json, request.Json));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_changelog", System.Data.DbType.String, request.ChangeLog));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@issave", System.Data.DbType.Boolean, request.IsSave));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@commit_uid", System.Data.DbType.Int32, request.UserId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@src_pid", System.Data.DbType.String, request.TenantAccountId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@cur_pid", System.Data.DbType.String, request.TenantAccountId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@relations", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, (request.Relations != null) ? request.Relations.Split(',').Select(n => n.ToString()).ToArray() : arr));

                    refId = cmd.ExecuteScalar().ToString();

                    if (request.EbObjectType == (int)EbObjectType.DataVisualization)
                        this.Redis.Set<EbDataVisualization>(refId, _eb_object as EbDataVisualization);
                    if (request.EbObjectType == (int)EbObjectType.DataSource)
                        this.Redis.Set<EbDataSource>(refId, _eb_object as EbDataSource);
                    if (request.EbObjectType == (int)EbObjectType.FilterDialog)
                        this.Redis.Set<EbFilterDialog>(refId, _eb_object as EbFilterDialog);

                }
            }
            catch (Exception e)
            {

            }
            return new EbObjectSubsequentCommitResponse() { RefId = refId };
        }

        public EbObject_CommitResponse Post(EbObject_CommitRequest request)
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

                    string sql = "SELECT eb_objects_commit(@id, @obj_name, @obj_desc, @obj_type, @obj_json, @obj_changelog,  @commit_uid, @src_pid, @cur_pid, @relations)";
                    cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);
                   
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.String, request.RefId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_name", System.Data.DbType.String, request.Name));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_type", System.Data.DbType.Int32, (int)request.EbObjectType));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_desc", System.Data.DbType.String, (!string.IsNullOrEmpty(request.Description)) ? request.Description : string.Empty));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_json", NpgsqlTypes.NpgsqlDbType.Json, request.Json));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_changelog", System.Data.DbType.String, request.ChangeLog));
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
            return new EbObject_CommitResponse() { RefId = refId };
        }

        public EbObject_SaveResponse Post(EbObject_SaveRequest request)
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

                    string sql = "SELECT eb_objects_save(@id, @obj_name, @obj_desc, @obj_type, @obj_json, @commit_uid, @src_pid, @cur_pid, @relations)";
                    cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);
                  
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.String, request.RefId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_name", System.Data.DbType.String, request.Name));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_type", System.Data.DbType.Int32, (int)request.EbObjectType));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_desc", System.Data.DbType.String, (!string.IsNullOrEmpty(request.Description)) ? request.Description : string.Empty));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_json", NpgsqlTypes.NpgsqlDbType.Json, request.Json));
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
            return new EbObject_SaveResponse() { RefId = refId };
        }

        public EbObject_Create_New_ObjectResponse Post(EbObject_Create_New_ObjectRequest request)
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

                    string sql = "SELECT eb_objects_create_new_object(@obj_name, @obj_desc, @obj_type, @obj_cur_status, @obj_json, @commit_uid, @src_pid, @cur_pid, @relations)";
                    cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);

                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_name", System.Data.DbType.String, request.Name));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_type", System.Data.DbType.Int32, (int)request.EbObjectType));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_desc", System.Data.DbType.String, (!string.IsNullOrEmpty(request.Description)) ? request.Description : string.Empty));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_cur_status", System.Data.DbType.Int32, ObjectLifeCycleStatus.Development));//request.Status
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@obj_json", NpgsqlTypes.NpgsqlDbType.Json, request.Json));
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
    }
}