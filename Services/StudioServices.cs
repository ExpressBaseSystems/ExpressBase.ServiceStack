using System.Collections.Generic;
using ServiceStack;
using System;
using ExpressBase.Objects;
using System.Data.Common;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack.Logging;
using System.Linq;
using ExpressBase.Common;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Data;
using ExpressBase.Objects.EmailRelated;
using System.Text.RegularExpressions;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.Objects;
using ExpressBase.ServiceStack.Services;
using ExpressBase.Objects.Objects.SmsRelated;

namespace ExpressBase.ServiceStack
{
    [ClientCanSwapTemplates]
    [DefaultView("Form")]
    [Authenticate]
    public class EbObjectService : EbBaseService
    {
        public EbObjectService(IEbConnectionFactory _dbf) : base(_dbf) { }

        [CompressResponse]
        public object Get(EbObjectAllVersionsRequest request) // Fetch all version without json of a particular Object
        {
            List<EbObjectWrapper> wrap = new List<EbObjectWrapper>();
            List<DbParameter> parameters = new List<DbParameter>();
            parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("refid", EbDbTypes.String, request.RefId));
            EbDataTable dt = EbConnectionFactory.ObjectsDB.DoQuery(EbConnectionFactory.ObjectsDB.EB_FETCH_ALL_VERSIONS_OF_AN_OBJ, parameters.ToArray());

            foreach (EbDataRow dr in dt.Rows)
            {
                EbObjectWrapper _ebObject = (new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    VersionNumber = dr[1].ToString(),
                    ChangeLog = dr[2].ToString(),
                    CommitTs = Convert.ToDateTime((dr[3].ToString()) == "0" || (dr[3].ToString()) == "" ? DateTime.MinValue : dr[3]),
                    RefId = dr[4].ToString(),
                    CommitUId = Convert.ToInt32(dr[5]),
                    CommitUname = dr[6].ToString()
                });
                wrap.Add(_ebObject);
            }
            return new EbObjectAllVersionsResponse { Data = wrap };
        }

        [CompressResponse]
        public object Get(EbObjectParticularVersionRequest request)// Fetch particular version with json of a particular Object
        {
            List<EbObjectWrapper> wrap = new List<EbObjectWrapper>();
            DbParameter[] parameters = { EbConnectionFactory.ObjectsDB.GetNewParameter("refid", EbDbTypes.String, request.RefId) };
            EbDataTable dt = EbConnectionFactory.ObjectsDB.DoQuery(EbConnectionFactory.ObjectsDB.EB_PARTICULAR_VERSION_OF_AN_OBJ, parameters);

            foreach (EbDataRow dr in dt.Rows)
            {
                EbObjectWrapper _ebObject = (new EbObjectWrapper
                {
                    Json = dr[0].ToString(),
                    VersionNumber = dr[1].ToString(),
                    EbObjectType = (dr[4] != DBNull.Value) ? Convert.ToInt32(dr[4]) : 0,
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), Convert.ToInt32(dr[2])),
                    Tags = dr[3].ToString(),
                    RefId = request.RefId
                });
                wrap.Add(_ebObject);
            }
            return new EbObjectParticularVersionResponse { Data = wrap };
        }

        [CompressResponse]
        public object Any(EbObjectParticularVersionRequest request)
        {
            return this.Get(request);
        }

        [CompressResponse]
        public object Get(EbObjectLatestCommitedRequest request) // Fetch latest committed version with json - for Execute/Run/Consume a particular Object
        {
            List<EbObjectWrapper> wrap = new List<EbObjectWrapper>();
            DbParameter[] parameters = { EbConnectionFactory.ObjectsDB.GetNewParameter("refid", EbDbTypes.String, request.RefId) };
            EbDataTable dt = EbConnectionFactory.ObjectsDB.DoQuery(EbConnectionFactory.ObjectsDB.EB_LATEST_COMMITTED_VERSION_OF_AN_OBJ, parameters);

            foreach (EbDataRow dr in dt.Rows)
            {
                EbObjectWrapper _ebObject = (new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString(),
                    EbObjectType = ((EbObjectType)Convert.ToInt32(dr[3])).IntCode,
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), Convert.ToInt32(dr[4])),
                    Description = dr[5].ToString(),
                    VersionNumber = dr[8].ToString(),
                    Json = (!string.IsNullOrEmpty(request.RefId)) ? dr[12].ToString() : null,
                    RefId = dr[13].ToString()
                });
                wrap.Add(_ebObject);
            }
            return new EbObjectLatestCommitedResponse { Data = wrap };
        }

        [CompressResponse]
        public object Get(EbObjectObjListRequest request)// Get All latest committed versions of this Object Type without json
        {
            List<EbObjectWrapper> wrap = new List<EbObjectWrapper>();
            DbParameter[] parameters = { EbConnectionFactory.ObjectsDB.GetNewParameter("type", EbDbTypes.Int32, request.EbObjectType) };
            EbDataTable dt = EbConnectionFactory.ObjectsDB.DoQuery(EbConnectionFactory.ObjectsDB.EB_ALL_LATEST_COMMITTED_VERSION_OF_AN_OBJ, parameters);

            foreach (EbDataRow dr in dt.Rows)
            {
                EbObjectWrapper _ebObject = (new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString() + dr[7].ToString(),
                    EbObjectType = ((EbObjectType)Convert.ToInt32(dr[3])).IntCode,
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), Convert.ToInt32(dr[3])),
                    Description = dr[4].ToString(),
                    VersionNumber = dr[7].ToString(),
                    CommitTs = Convert.ToDateTime((dr[9].ToString()) == "" || (dr[9].ToString()) == "0" ? DateTime.MinValue : dr[9]),
                    RefId = dr[11].ToString(),
                    CommitUname = dr[12].ToString(),
                });
                wrap.Add(_ebObject);
            }
            return new EbObjectObjListResponse { Data = wrap };
        }

        [CompressResponse]
        public object Get(EbObjectListRequest request)// Get All latest committed versions of this Object Type without json
        {
            List<EbObjectWrapper> wrap = new List<EbObjectWrapper>();
            DbParameter[] parameters = { EbConnectionFactory.ObjectsDB.GetNewParameter("type", EbDbTypes.Int32, request.EbObjectType) };
            EbDataTable dt = EbConnectionFactory.ObjectsDB.DoQuery(EbConnectionFactory.ObjectsDB.EB_GET_OBJ_LIST_FROM_EBOBJECTS, parameters);

            foreach (EbDataRow dr in dt.Rows)
            {
                EbObjectWrapper _ebObject = (new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString(),
                    EbObjectType = ((EbObjectType)Convert.ToInt32(dr[3])).IntCode,
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), Convert.ToInt32(dr[3])),
                    Description = dr[4].ToString()
                });
                wrap.Add(_ebObject);
            }
            return new EbObjectListResponse { Data = wrap };
        }

        [CompressResponse]
        public object Get(EbObjectObjLisAllVerRequest request)// Get All latest committed versions of this Object Type without json
        {
            List<EbObjectWrapper> wrap = new List<EbObjectWrapper>();
            DbParameter[] parameters = { EbConnectionFactory.ObjectsDB.GetNewParameter("type", EbDbTypes.Int32, request.EbObjectType) };
            EbDataTable dt = EbConnectionFactory.ObjectsDB.DoQuery(EbConnectionFactory.ObjectsDB.EB_GET_ALL_COMMITTED_VERSION_LIST, parameters);

            Dictionary<string, List<EbObjectWrapper>> f_dict = new Dictionary<string, List<EbObjectWrapper>>();
            List<EbObjectWrapper> wrap_list = null;
            foreach (EbDataRow dr in dt.Rows)
            {
                string _nameKey = dr[1].ToString();
                if (!f_dict.ContainsKey(_nameKey))
                {
                    wrap_list = new List<EbObjectWrapper>();
                    f_dict.Add(_nameKey, wrap_list);
                }

                wrap_list.Add(new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString(),
                    EbObjectType = ((EbObjectType)Convert.ToInt32(dr[3])).IntCode,
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), Convert.ToInt32(dr[3])),
                    VersionNumber = dr[7].ToString(),
                    RefId = dr[11].ToString(),
                    DisplayName = dr[13].ToString()
                });
            }
            return new EbObjectObjListAllVerResponse { Data = f_dict };
        }

        public EbObjAllVerWithoutCircularRefResp Get(EbObjAllVerWithoutCircularRefRqst request)// Get all latest committed versions of EbObject without json - circular reference situation avoided on query
        {
            string query = @"
SELECT 
    EO.id, EO.obj_name, EO.display_name, 
    EOV.id, EOV.version_num, EOV.refid
FROM
    eb_objects EO, eb_objects_ver EOV
WHERE
    EO.id = EOV.eb_objects_id AND
    EOV.working_mode = 'F' AND
    EO.obj_type = :obj_type 
ORDER BY 
    EO.display_name ASC, EOV.version_num DESC;
";
            List<DbParameter> parameters = new List<DbParameter>();
            if (!request.EbObjectRefId.IsNullOrEmpty())
            {
                query = @"
SELECT 
    EO.id, EO.obj_name, EO.display_name, 
    EOV.id, EOV.version_num, EOV.refid
FROM
    eb_objects EO, eb_objects_ver EOV
WHERE
    EO.id = EOV.eb_objects_id AND
    EOV.working_mode = 'F' AND
    EO.obj_type = :obj_type AND
    EOV.refid != :dominant AND
    EOV.refid NOT IN (
        WITH RECURSIVE objects_relations AS (
	    SELECT dependant FROM eb_objects_relations WHERE eb_del='F' AND dominant = :dominant
	    UNION
	    SELECT a.dependant FROM eb_objects_relations a, objects_relations b WHERE a.eb_del='F' AND a.dominant = b.dependant
        )SELECT * FROM objects_relations
    )
ORDER BY 
    EO.display_name ASC, EOV.version_num DESC;    
";
                parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("dominant", EbDbTypes.String, request.EbObjectRefId));
            }

            parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("obj_type", EbDbTypes.Int32, request.EbObjType));
            EbDataTable dt = EbConnectionFactory.ObjectsDB.DoQuery(query, parameters.ToArray());

            Dictionary<string, List<EbObjectWrapper>> obj_dict = new Dictionary<string, List<EbObjectWrapper>>();
            List<EbObjectWrapper> wrap_list = null;
            foreach (EbDataRow dr in dt.Rows)
            {
                string _nameKey = dr[1].ToString();
                if (!obj_dict.ContainsKey(_nameKey))
                {
                    wrap_list = new List<EbObjectWrapper>();
                    obj_dict.Add(_nameKey, wrap_list);
                }

                wrap_list.Add(new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = _nameKey,
                    DisplayName = dr[2].ToString(),
                    VersionNumber = dr[4].ToString(),
                    RefId = dr[5].ToString()
                });
            }

            return new EbObjAllVerWithoutCircularRefResp { Data = obj_dict };
        }

        [CompressResponse]
        public object Get(EbAllObjNVerRequest request)// Get All latest committed versions of this Object Type without json
        {
            EbDataTable dt;

            string query = @"SELECT 
                            EO.id, EO.obj_name, EO.obj_type, EO.obj_cur_status,EO.obj_desc,
                            EOV.id, EOV.eb_objects_id, EOV.version_num, EOV.obj_changelog, EOV.commit_ts, EOV.commit_uid, EOV.refid,
                            EU.fullname
                        FROM 
                            eb_objects EO, eb_objects_ver EOV
                        LEFT JOIN
	                        eb_users EU
                        ON 
	                        EOV.commit_uid=EU.id
                        WHERE
                            EO.id = ANY(string_to_array(:ids,',')::int[]) AND
                            EO.id = EOV.eb_objects_id AND COALESCE(EOV.working_mode, 'F') <> 'T'
                        ORDER BY
                            EO.obj_name;";

            DbParameter[] parameters = { EbConnectionFactory.ObjectsDB.GetNewParameter(":ids", EbDbTypes.String, request.ObjectIds) };
            dt = EbConnectionFactory.ObjectsDB.DoQuery(query, parameters);

            Dictionary<string, List<EbObjectWrapper>> f_dict = new Dictionary<string, List<EbObjectWrapper>>();
            List<EbObjectWrapper> wrap_list = null;
            foreach (EbDataRow dr in dt.Rows)
            {
                string _nameKey = dr[1].ToString();
                if (!f_dict.ContainsKey(_nameKey))
                {
                    wrap_list = new List<EbObjectWrapper>();
                    f_dict.Add(_nameKey, wrap_list);
                }

                wrap_list.Add(new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString(),
                    EbObjectType = ((EbObjectType)Convert.ToInt32(dr[3])).IntCode,
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), Convert.ToInt32(dr[3])),
                    VersionNumber = dr[7].ToString(),
                    RefId = dr[11].ToString(),
                });
            }
            return new EbObjectObjListAllVerResponse { Data = f_dict };
        }

        [CompressResponse]
        public object Get(EbObjectRelationsRequest request)//Fetch ebobjects relations           
        {
            List<EbObjectWrapper> wrap = new List<EbObjectWrapper>();
            DbParameter[] parameters = { EbConnectionFactory.ObjectsDB.GetNewParameter("dominant", EbDbTypes.String, request.DominantId) };
            EbDataTable dt = EbConnectionFactory.ObjectsDB.DoQuery(EbConnectionFactory.ObjectsDB.EB_GET_LIVE_OBJ_RELATIONS, parameters);
            foreach (EbDataRow dr in dt.Rows)
            {
                EbObjectWrapper _ebObject = new EbObjectWrapper
                {
                    Name = dr[0].ToString(),
                    RefId = dr[1].ToString(),
                    VersionNumber = dr[2].ToString(),
                    EbObjectType = ((EbObjectType)Convert.ToInt32(dr[3])).IntCode
                };

                wrap.Add(_ebObject);
            }

            return new EbObjectRelationsResponse { Data = wrap };
        }

        //Get Tagged Objects
        [CompressResponse]
        public object Get(EbObjectTaggedRequest request)
        {
            List<EbObjectWrapper> wrap = new List<EbObjectWrapper>();
            DbParameter[] parameters = { EbConnectionFactory.ObjectsDB.GetNewParameter("tags", EbDbTypes.String, request.Tags) };
            EbDataTable dt = EbConnectionFactory.ObjectsDB.DoQuery(EbConnectionFactory.ObjectsDB.EB_GET_TAGGED_OBJECTS, parameters);
            foreach (EbDataRow dr in dt.Rows)
            {
                EbObjectWrapper _ebObject = new EbObjectWrapper
                {
                    Name = dr[0].ToString(),
                    RefId = dr[1].ToString(),
                    VersionNumber = dr[2].ToString(),
                    EbObjectType = ((EbObjectType)Convert.ToInt32(dr[3])).IntCode
                };

                wrap.Add(_ebObject);
            }

            return new EbObjectTaggedResponse { Data = wrap };
        }

        //Get the version to open ion edit mode
        [CompressResponse]
        public object Get(EbObjectExploreObjectRequest request)
        {
            List<EbObjectWrapper> wrap = new List<EbObjectWrapper>();
            DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.Id) };
            EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery(EbConnectionFactory.ObjectsDB.EB_EXPLORE_OBJECT, parameters);
            foreach (EbDataRow dr in dt.Rows)
            {
                try
                {
                    EbObjectWrapper _ebObject = (new EbObjectWrapper
                    {
                        Id = Convert.ToInt32(dr[0]),
                        Name = dr[1].ToString(),
                        EbObjectType = ((EbObjectType)Convert.ToInt32(dr[2])).IntCode,
                        Status = Enum.GetName(typeof(ObjectLifeCycleStatus), Convert.ToInt32(dr[3])),
                        Description = dr[4].ToString(),
                        ChangeLog = dr[5].ToString(),
                        CommitTs = Convert.ToDateTime((dr[6].ToString()) == "0" || (dr[6].ToString()) == "" ? DateTime.MinValue : dr[6]),
                        CommitUname = dr[7].ToString(),
                        RefId = dr[8].ToString(),
                        VersionNumber = dr[9].ToString(),
                        WorkingMode = (dr[10].ToString() == "T") ? true : false,
                        Json_wc = dr[12] as string,
                        Json_lc = dr[13] as string,
                        Wc_All = (dr[11] as string == null) ? (dr[11] as string[]) : (dr[11] as string).Split(","),
                        Tags = dr[17].ToString(),
                        Apps = dr[18].ToString().Replace("\n", "").Replace("\t", "").Replace("\r", ""),
                        Dashboard_Tiles = new EbObj_Dashboard
                        {
                            MajorVersionNumber = Convert.ToInt32(dr[14]),
                            MinorVersionNumber = Convert.ToInt32(dr[15]),
                            PatchVersionNumber = Convert.ToInt32(dr[16]),
                            LastCommitedVersionRefid = dr[19].ToString(),
                            LastCommitedVersionNumber = dr[20].ToString(),
                            LastCommitedVersionCommit_ts = Convert.ToDateTime((dr[21].ToString()) == "0" || (dr[21].ToString()) == "" ? DateTime.MinValue : dr[21]),
                            LastCommitedVersion_Status = Enum.GetName(typeof(ObjectLifeCycleStatus), Convert.ToInt32(dr[22])),
                            LastCommitedby_Name = dr[23].ToString(),
                            LastCommitedby_Id = Convert.ToInt32(dr[24]),
                            LiveVersionRefid = dr[25].ToString(),
                            LiveVersionNumber = dr[26].ToString(),
                            LiveVersionCommit_ts = Convert.ToDateTime((dr[27].ToString()) == "0" || (dr[27].ToString()) == "" ? DateTime.MinValue : dr[27]),
                            LiveVersion_Status = Enum.GetName(typeof(ObjectLifeCycleStatus), Convert.ToInt32(dr[28])),
                            LiveVersionCommitby_Name = dr[29].ToString(),
                            LiveVersionCommitby_Id = Convert.ToInt32(dr[30]),
                            OwnerUid = Convert.ToInt32(dr[31]),
                            OwnerTs = Convert.ToDateTime((dr[32].ToString()) == "0" || (dr[32].ToString()) == "" ? DateTime.MinValue : dr[32]),
                            OwnerName = dr[33].ToString()
                        },
                        DisplayName = dr[34].ToString(),
                        IsLogEnabled = Convert.ToBoolean(dr[35])
                    });

                    wrap.Add(_ebObject);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception: " + e.ToString());
                }

            }
            return new EbObjectExploreObjectResponse { Data = wrap };
        }

        [CompressResponse]
        public object Get(EbObjectUpdateDashboardRequest request)
        {
            List<EbObjectWrapper> wrap = new List<EbObjectWrapper>();
            DbParameter[] parameters = { EbConnectionFactory.ObjectsDB.GetNewParameter("refid", EbDbTypes.String, request.Refid) };
            EbDataTable dt = EbConnectionFactory.ObjectsDB.DoQuery(EbConnectionFactory.ObjectsDB.EB_UPDATE_DASHBOARD, parameters);

            foreach (EbDataRow dr in dt.Rows)
            {
                try
                {
                    EbObjectWrapper _ebObject = (new EbObjectWrapper
                    {
                        Name = dr[0].ToString(),
                        Status = Enum.GetName(typeof(ObjectLifeCycleStatus), Convert.ToInt32(dr[1])),
                        RefId = request.Refid,
                        VersionNumber = dr[2].ToString(),
                        WorkingMode = (dr[10].ToString() == "T") ? true : false,
                        Wc_All = (dr[4] as string == null) ? (dr[4] as string[]) : (dr[4] as string).Split(","),
                        Tags = dr[8].ToString(),
                        Apps = dr[9].ToString().Replace("\n", "").Replace("\t", "").Replace("\r", ""),
                        Dashboard_Tiles = new EbObj_Dashboard
                        {
                            MajorVersionNumber = Convert.ToInt32(dr[5]),
                            MinorVersionNumber = Convert.ToInt32(dr[6]),
                            PatchVersionNumber = Convert.ToInt32(dr[7]),
                            LastCommitedVersionRefid = dr[10].ToString(),
                            LastCommitedVersionNumber = dr[11].ToString(),
                            LastCommitedVersionCommit_ts = Convert.ToDateTime((dr[12].ToString()) == "0" || (dr[12].ToString()) == "" ? DateTime.MinValue : dr[12]),
                            LastCommitedVersion_Status = Enum.GetName(typeof(ObjectLifeCycleStatus), Convert.ToInt32(dr[13])),
                            LastCommitedby_Name = dr[14].ToString(),
                            LastCommitedby_Id = Convert.ToInt32(dr[15]),
                            LiveVersionRefid = dr[16].ToString(),
                            LiveVersionNumber = dr[17].ToString(),
                            LiveVersionCommit_ts = Convert.ToDateTime((dr[18].ToString()) == "0" || (dr[18].ToString()) == "" ? DateTime.MinValue : dr[18]),
                            LiveVersion_Status = Enum.GetName(typeof(ObjectLifeCycleStatus), Convert.ToInt32(dr[19])),
                            LiveVersionCommitby_Name = dr[20].ToString(),
                            LiveVersionCommitby_Id = Convert.ToInt32(dr[21]),
                            OwnerUid = Convert.ToInt32(dr[22]),
                            OwnerTs = Convert.ToDateTime((dr[23].ToString()) == "" || (dr[23].ToString()) == "" ? DateTime.MinValue : dr[23]),
                            OwnerName = dr[24].ToString()
                        }
                    });
                    wrap.Add(_ebObject);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception: " + e.ToString());
                }
            }
            return new EbObjectUpdateDashboardResponse { Data = wrap };
        }

        [CompressResponse]
        public object Get(EbObjectStatusHistoryRequest request)
        { // Get All latest committed versions of this Object Type without json
            List<EbObjectWrapper> wrap = new List<EbObjectWrapper>();
            DbParameter[] parameters = { EbConnectionFactory.ObjectsDB.GetNewParameter("refid", EbDbTypes.String, request.RefId) };
            EbDataTable dt = EbConnectionFactory.ObjectsDB.DoQuery(EbConnectionFactory.ObjectsDB.EB_GET_OBJ_STATUS_HISTORY, parameters);

            foreach (EbDataRow dr in dt.Rows)
            {
                EbObjectWrapper _ebObject = (new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), Convert.ToInt32(dr[1])),
                    CommitUname = dr[2].ToString(),
                    CommitTs = Convert.ToDateTime((dr[3].ToString()) == "" || (dr[3].ToString()) == "0" ? DateTime.MinValue : dr[3]),
                    ChangeLog = dr[4].ToString(),
                    CommitUId = Convert.ToInt32(dr[5])
                });
                wrap.Add(_ebObject);
            }
            return new EbObjectStatusHistoryResponse { Data = wrap };
            //
        }

        [CompressResponse]
        public object Get(EbObjectFetchLiveVersionRequest request) // Fetch particular version with json of a particular Object
        {
            List<EbObjectWrapper> wrap = new List<EbObjectWrapper>();
            DbParameter[] parameters = { EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.Id) };
            EbDataTable dt = EbConnectionFactory.ObjectsDB.DoQuery(EbConnectionFactory.ObjectsDB.EB_LIVE_VERSION_OF_OBJS, parameters);

            foreach (EbDataRow dr in dt.Rows)
            {
                EbObjectWrapper _ebObject = (new EbObjectWrapper
                {
                    RefId = dr[11].ToString(),
                    Name = dr[1].ToString(),
                    EbObjectType = Convert.ToInt32(dr[2]),
                    VersionNumber = dr[6].ToString(),
                    Json = dr[10].ToString()
                });
                wrap.Add(_ebObject);
            }
            return new EbObjectFetchLiveVersionResponse { Data = wrap };
        }

        [CompressResponse]
        public object Get(EbObjectGetAllTagsRequest request)
        {
            string tags = string.Empty;
            ILog log = LogManager.GetLogger(GetType());
            EbDataTable dt = EbConnectionFactory.ObjectsDB.DoQuery(EbConnectionFactory.ObjectsDB.EB_GET_ALL_TAGS);
            foreach (EbDataRow dr in dt.Rows)
            {
                tags += dr[0].ToString() + ",";
            }

            return new EbObjectGetAllTagsResponse { Data = tags };
        }

        [CompressResponse]
        public UniqueObjectNameCheckResponse Get(UniqueObjectNameCheckRequest request)
        {
            DbParameter[] parameters = { EbConnectionFactory.ObjectsDB.GetNewParameter("name", EbDbTypes.String, request.ObjName) };
            EbDataTable dt = EbConnectionFactory.ObjectsDB.DoQuery("SELECT id FROM eb_objects WHERE obj_name = :name ;", parameters);
            bool _isunique = (dt.Rows.Count > 0) ? false : true;
            return new UniqueObjectNameCheckResponse { IsUnique = _isunique };
        }

        // [Authenticate]
        public EbObject_CommitResponse Post(EbObject_CommitRequest request)
        {
            EbObject obj = EbSerializers.Json_Deserialize(request.Json);
            string refId = null;
            ILog log = LogManager.GetLogger(GetType());
            log.Info("Commit Object-- started");

            try
            {
                using (DbConnection con = EbConnectionFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    DbCommand cmd = null;
                    string sql = EbConnectionFactory.ObjectsDB.EB_COMMIT_OBJECT;
                    cmd = EbConnectionFactory.ObjectsDB.GetNewCommand(con, sql);

                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":id", EbDbTypes.String, request.RefId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_name", EbDbTypes.String, request.Name.Replace("\n", "").Replace("\t", "").Replace("\r", "")));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_desc", EbDbTypes.String, (!string.IsNullOrEmpty(request.Description)) ? request.Description : string.Empty));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_type", EbDbTypes.Int32, GetObjectType(obj)));
                    //cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_json", EbDbTypes.Json, request.Json));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_changelog", EbDbTypes.String, request.ChangeLog));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":commit_uid", EbDbTypes.Int32, request.UserId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":relations", EbDbTypes.String, (request.Relations != null) ? request.Relations : string.Empty));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":tags", EbDbTypes.String, (!string.IsNullOrEmpty(request.Tags)) ? request.Tags : string.Empty));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":app_id", EbDbTypes.String, SetAppId(request.Apps)));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":disp_name", EbDbTypes.String, request.DisplayName));

                    if (sql.Contains(":obj_json"))
                    {
                        cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_json", EbDbTypes.Json, request.Json));
                        refId = cmd.ExecuteScalar().ToString();
                    }
                    else
                    {
                        refId = cmd.ExecuteScalar().ToString();
                        string sql1 = "update eb_objects_ver set obj_json=:jsonobj where refid=:refid";

                        NTV[] parms = new NTV[2];
                        parms[0] = new NTV() { Name = ":jsonobj", Type = EbDbTypes.Json, Value = request.Json };
                        parms[1] = new NTV { Name = ":refid", Type = EbDbTypes.String, Value = refId };

                        Update_Json_Val(con, sql1, parms);
                    }
                    SetRedis(obj, refId);

                    // need optimization
                    if (obj is EbBotForm)
                    {
                        ChatbotServices myService = base.ResolveService<ChatbotServices>();
                        CreateBotFormTableResponse res = (CreateBotFormTableResponse)myService.Any(new CreateBotFormTableRequest() { BotObj = obj, Apps = request.Apps, SolnId = request.SolnId, UserId = request.UserId, WhichConsole = request.WhichConsole });
                    }
                    else if (obj is EbWebForm)
                    {
                        WebFormServices myService = base.ResolveService<WebFormServices>();
                        CreateWebFormTableResponse res = (CreateWebFormTableResponse)myService.Any(new CreateWebFormTableRequest() { WebObj = obj as EbWebForm, Apps = request.Apps, SolnId = request.SolnId, UserId = request.UserId, WhichConsole = request.WhichConsole });
                    }
                    else if (obj is EbSqlFunction)
                    {
                        RunSqlFunctionResponse resp = this.Post(new RunSqlFunctionRequest
                        {
                            SolnId = request.SolnId,
                            UserId = request.UserId,
                            WhichConsole = request.WhichConsole,
                            UserAuthId = request.UserAuthId,
                            Json = request.Json
                        });
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.ToString());
            }
            return new EbObject_CommitResponse() { RefId = refId };
        }

        public EbObject_SaveResponse Post(EbObject_SaveRequest request)
        {
            EbObject obj = EbSerializers.Json_Deserialize(request.Json);
            string refId = null;
            ILog log = LogManager.GetLogger(GetType());
            log.Info("Save Object -- started");
            try
            {
                using (DbConnection con = EbConnectionFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    DbCommand cmd = null;
                    string sql = EbConnectionFactory.ObjectsDB.EB_SAVE_OBJECT;
                    cmd = EbConnectionFactory.ObjectsDB.GetNewCommand(con, sql);

                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":id", EbDbTypes.String, request.RefId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_name", EbDbTypes.String, request.Name.Replace("\n", "").Replace("\t", "").Replace("\r", "")));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_desc", EbDbTypes.String, (!string.IsNullOrEmpty(request.Description)) ? request.Description : string.Empty));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_type", EbDbTypes.Int32, GetObjectType(obj)));
                    //cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_json", EbDbTypes.Json, request.Json));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":commit_uid", EbDbTypes.Int32, request.UserId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":relations", EbDbTypes.String, (request.Relations != null) ? request.Relations : string.Empty));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":tags", EbDbTypes.String, (!string.IsNullOrEmpty(request.Tags)) ? request.Tags : string.Empty));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":app_id", EbDbTypes.String, SetAppId(request.Apps)));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":disp_name", EbDbTypes.String, request.DisplayName));


                    if (sql.Contains(":obj_json"))
                    {
                        cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_json", EbDbTypes.Json, request.Json));
                        refId = cmd.ExecuteScalar().ToString();
                    }
                    else
                    {
                        refId = cmd.ExecuteScalar().ToString();
                        string sql1 = "update eb_objects_ver set obj_json=:jsonobj where refid=:refid";

                        NTV[] parms = new NTV[2];
                        parms[0] = new NTV() { Name = ":jsonobj", Type = EbDbTypes.Json, Value = request.Json };
                        parms[1] = new NTV { Name = ":refid", Type = EbDbTypes.String, Value = refId };

                        Update_Json_Val(con, sql1, parms);
                    }

                    SetRedis(obj, refId);
                    if (obj is EbBotForm)
                    {
                        ChatbotServices myService = base.ResolveService<ChatbotServices>();
                        CreateBotFormTableResponse res = (CreateBotFormTableResponse)myService.Any(new CreateBotFormTableRequest() { BotObj = obj, Apps = request.Apps, SolnId = request.SolnId, UserId = request.UserId, WhichConsole = request.WhichConsole });
                    }
                    else if (obj is EbWebForm)
                    {
                        WebFormServices myService = base.ResolveService<WebFormServices>();
                        CreateWebFormTableResponse res = (CreateWebFormTableResponse)myService.Any(new CreateWebFormTableRequest() { WebObj = obj as EbWebForm, Apps = request.Apps, SolnId = request.SolnId, UserId = request.UserId, WhichConsole = request.WhichConsole });
                    }
                    else if (obj is EbSqlFunction)
                    {
                        RunSqlFunctionResponse resp = this.Post(new RunSqlFunctionRequest
                        {
                            SolnId = request.SolnId,
                            UserId = request.UserId,
                            WhichConsole = request.WhichConsole,
                            UserAuthId = request.UserAuthId,
                            Json = request.Json
                        });
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.ToString());
            }
            return new EbObject_SaveResponse() { RefId = refId };
        }

        public EbObject_Create_New_ObjectResponse Post(EbObject_Create_New_ObjectRequest request)
        {
            dynamic obj = EbSerializers.Json_Deserialize(request.Json);
            //dynamic _type = obj.GetType();
            string refId = null;
            string exception_msg = string.Empty;
            ILog log = LogManager.GetLogger(GetType());

            try
            {
                using (DbConnection con = EbConnectionFactory.ObjectsDB.GetNewConnection())
                {
                    //refId = StaticMethods.CreateObject(EbConnectionFactory, request, SetAppId(request.Apps));
                    {
                        con.Open();
                        DbCommand cmd = null;
                        log.Info("Create new object -- started");
                        string[] arr = { };

                        String sql = EbConnectionFactory.ObjectsDB.EB_CREATE_NEW_OBJECT;
                        cmd = EbConnectionFactory.ObjectsDB.GetNewCommand(con, sql);

                        cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_name", EbDbTypes.String, request.Name.Replace("\n", "").Replace("\t", "").Replace("\r", "")));
                        cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_desc", EbDbTypes.String, (!string.IsNullOrEmpty(request.Description)) ? request.Description : string.Empty));
                        cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_type", EbDbTypes.Int32, GetObjectType(obj)));
                        cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_cur_status", EbDbTypes.Int32, (int)request.Status));//request.Status
                        //cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_json", EbDbTypes.Json, request.Json));
                        cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":commit_uid", EbDbTypes.Int32, request.UserId));
                        cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":src_pid", EbDbTypes.String, request.SourceSolutionId));
                        cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":cur_pid", EbDbTypes.String, request.SolnId));
                        cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":relations", EbDbTypes.String, (request.Relations != null) ? request.Relations : string.Empty));
                        cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":issave", EbDbTypes.String, (request.IsSave == true) ? 'T' : 'F'));
                        cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":tags", EbDbTypes.String, (!string.IsNullOrEmpty(request.Tags)) ? request.Tags : string.Empty));
                        cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":app_id", EbDbTypes.String, SetAppId(request.Apps)));
                        cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":s_obj_id", EbDbTypes.String, request.SourceObjId));
                        cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":s_ver_id", EbDbTypes.String, request.SourceVerID));
                        cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":disp_name", EbDbTypes.String, request.DisplayName));

                        if (sql.Contains(":obj_json"))
                        {
                            cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_json", EbDbTypes.Json, request.Json));
                            refId = cmd.ExecuteScalar().ToString();
                        }
                        else
                        {
                            refId = cmd.ExecuteScalar().ToString();
                            string sql1 = "update eb_objects_ver set obj_json=:jsonobj where refid=:refid";

                            NTV[] parms = new NTV[2];
                            parms[0] = new NTV() { Name = ":jsonobj", Type = EbDbTypes.Json, Value = request.Json };
                            parms[1] = new NTV { Name = ":refid", Type = EbDbTypes.String, Value = refId };

                            Update_Json_Val(con, sql1, parms);
                        }
                    }
                    SetRedis(obj, refId);
                    if (obj is EbBotForm)
                    {
                        ChatbotServices myService = base.ResolveService<ChatbotServices>();
                        CreateBotFormTableResponse res = (CreateBotFormTableResponse)myService.Any(new CreateBotFormTableRequest() { BotObj = obj, Apps = request.Apps, SolnId = request.SolnId, UserId = request.UserId, WhichConsole = request.WhichConsole });
                    }
                    else if (obj is EbWebForm)
                    {
                        WebFormServices myService = base.ResolveService<WebFormServices>();
                        CreateWebFormTableResponse res = (CreateWebFormTableResponse)myService.Any(new CreateWebFormTableRequest() { WebObj = obj, Apps = request.Apps, SolnId = request.SolnId, UserId = request.UserId, WhichConsole = request.WhichConsole });
                    }
                    else if (obj is EbSqlFunction)
                    {
                        RunSqlFunctionResponse resp = this.Post(new RunSqlFunctionRequest
                        {
                            SolnId = request.SolnId,
                            UserId = request.UserId,
                            WhichConsole = request.WhichConsole,
                            UserAuthId = request.UserAuthId,
                            Json = request.Json
                        });
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.ToString());
                if (e is Npgsql.PostgresException)
                {
                    if ((e as Npgsql.PostgresException).SqlState == "23505")
                        exception_msg = "The Operation Can't be completed because an item with the name \"" + request.Name + "\"" + " already exists. Specify a diffrent name.";
                }
            }
            return new EbObject_Create_New_ObjectResponse() { RefId = refId, ExceptionMessage = exception_msg };
        }

        public EbObject_Create_Major_VersionResponse Post(EbObject_Create_Major_VersionRequest request)
        {
            string refId = null;
            ILog log = LogManager.GetLogger(GetType());
            log.Info("Create major version -- started");

            try
            {
                using (DbConnection con = EbConnectionFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    DbCommand cmd = null;
                    string[] arr = { };
                    string sql = EbConnectionFactory.ObjectsDB.EB_MAJOR_VERSION_OF_OBJECT;
                    cmd = EbConnectionFactory.ObjectsDB.GetNewCommand(con, sql);

                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":id", EbDbTypes.String, request.RefId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_type", EbDbTypes.Int32, (int)request.EbObjectType));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":commit_uid", EbDbTypes.Int32, request.UserId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":src_pid", EbDbTypes.String, request.SolnId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":cur_pid", EbDbTypes.String, request.SolnId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":relations", EbDbTypes.String, (request.Relations != null) ? request.Relations : string.Empty));

                    refId = cmd.ExecuteScalar().ToString();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.ToString());
            }
            return new EbObject_Create_Major_VersionResponse() { RefId = refId };
        }

        public EbObject_Create_Minor_VersionResponse Post(EbObject_Create_Minor_VersionRequest request)
        {
            string refId = null;
            ILog log = LogManager.GetLogger(GetType());
            log.Info("Create minor version -- started");

            try
            {
                using (DbConnection con = EbConnectionFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    DbCommand cmd = null;
                    string[] arr = { };
                    string sql = EbConnectionFactory.ObjectsDB.EB_MINOR_VERSION_OF_OBJECT;
                    cmd = EbConnectionFactory.ObjectsDB.GetNewCommand(con, sql);

                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":id", EbDbTypes.String, request.RefId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_type", EbDbTypes.Int32, (int)request.EbObjectType));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":commit_uid", EbDbTypes.Int32, request.UserId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":src_pid", EbDbTypes.String, request.SolnId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":cur_pid", EbDbTypes.String, request.SolnId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":relations", EbDbTypes.String, (request.Relations != null) ? request.Relations : string.Empty));

                    refId = cmd.ExecuteScalar().ToString();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.ToString());
            }
            return new EbObject_Create_Minor_VersionResponse() { RefId = refId };
        }

        public EbObject_Create_Patch_VersionResponse Post(EbObject_Create_Patch_VersionRequest request)
        {
            string refId = null;
            ILog log = LogManager.GetLogger(GetType());
            log.Info("Create patch version -- started");
            try
            {
                using (DbConnection con = EbConnectionFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    DbCommand cmd = null;
                    string[] arr = { };

                    string sql = EbConnectionFactory.ObjectsDB.EB_PATCH_VERSION_OF_OBJECT;
                    cmd = EbConnectionFactory.ObjectsDB.GetNewCommand(con, sql);

                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":id", EbDbTypes.String, request.RefId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_type", EbDbTypes.Int32, (int)request.EbObjectType));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":commit_uid", EbDbTypes.Int32, request.UserId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":src_pid", EbDbTypes.String, request.SolnId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":cur_pid", EbDbTypes.String, request.SolnId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":relations", EbDbTypes.String, (request.Relations != null) ? request.Relations : string.Empty));

                    refId = cmd.ExecuteScalar().ToString();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.ToString());
            }
            return new EbObject_Create_Patch_VersionResponse() { RefId = refId };
        }

        public void Update_Json_Val(DbConnection con, String qry, NTV[] param)
        {
            try
            {
                DbTransaction transaction = con.BeginTransaction();
                DbCommand cmnd = null;
                cmnd = con.CreateCommand();
                cmnd.Transaction = transaction;
                cmnd.CommandText = qry;

                foreach (NTV para in param)
                {
                    DbParameter parm = EbConnectionFactory.ObjectsDB.GetNewParameter(para.Name, para.Type);
                    parm.Value = para.Value;
                    cmnd.Parameters.Add(parm);
                }

                cmnd.ExecuteNonQuery();
                cmnd.Transaction.Commit();
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.ToString());
            }
        }

        public RunSqlFunctionResponse Post(RunSqlFunctionRequest request)
        {
            ILog log = LogManager.GetLogger(GetType());
            log.Info("Run sql -- started");

            using (DbConnection con = EbConnectionFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                DbCommand cmd = null;
                int status = 0;
                try
                {
                    string code = EbSerializers.Json_Deserialize<EbSqlFunction>(request.Json).Sql;
                    cmd = EbConnectionFactory.ObjectsDB.GetNewCommand(con, code);
                    status = cmd.ExecuteNonQuery();
                }
                catch (Exception e)
                {
                    status = 0;
                }
                return new RunSqlFunctionResponse() { Status = status };
            };
        }

        public EbObjectChangeStatusResponse Post(EbObjectChangeStatusRequest request)
        {
            int id = 0;
            try
            {
                using (DbConnection con = EbConnectionFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    DbCommand cmd = null;
                    string sql = EbConnectionFactory.ObjectsDB.EB_CHANGE_STATUS_OBJECT;
                    cmd = EbConnectionFactory.ObjectsDB.GetNewCommand(con, sql);

                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":id", EbDbTypes.String, request.RefId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":status", EbDbTypes.Int32, (int)request.Status));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":commit_uid", EbDbTypes.Int32, request.UserId));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":obj_changelog", EbDbTypes.String, request.ChangeLog));
                    id = (int)cmd.ExecuteScalar();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.ToString());
            }
            return new EbObjectChangeStatusResponse { Id = id };
        }

        public DeleteObjectResponse Post(DeleteEbObjectRequest request)
        {
            string sql = "UPDATE eb_objects SET eb_del='T' WHERE id = :id";
            DbParameter[] p = { EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.ObjId) };
            int _rows = EbConnectionFactory.ObjectsDB.DoNonQuery(sql, p);
            return new DeleteObjectResponse { RowsDeleted = _rows };
        }
        public int GetObjectType(object obj)
        {
            if (obj is EbDataReader)
                return EbObjectTypes.DataReader.IntCode;
            else if (obj is EbTableVisualization)
                return EbObjectTypes.TableVisualization.IntCode;
            else if (obj is EbChartVisualization)
                return EbObjectTypes.ChartVisualization.IntCode;
            else if (obj is EbWebForm)
                return EbObjectTypes.WebForm.IntCode;
            else if (obj is EbUserControl)
                return EbObjectTypes.UserControl.IntCode;
            else if (obj is EbReport)
                return EbObjectTypes.Report.IntCode;
            else if (obj is EbFilterDialog)
                return EbObjectTypes.FilterDialog.IntCode;
            else if (obj is EbEmailTemplate)
                return EbObjectTypes.EmailBuilder.IntCode;
            else if (obj is EbBotForm)
                return EbObjectTypes.BotForm.IntCode;
            else if (obj is EbSmsTemplate)
                return EbObjectTypes.SmsBuilder.IntCode;
            else if (obj is EbDataWriter)
                return EbObjectTypes.DataWriter.IntCode;
            else if (obj is EbSqlFunction)
                return EbObjectTypes.SqlFunction.IntCode;
            else if (obj is EbApi)
                return EbObjectTypes.Api.IntCode;
            else
                return -1;
        }

        public void SetRedis(object obj, string refId)
        {
            if (obj is EbFilterDialog)
            {
                Redis.Set(refId, (EbFilterDialog)obj);
            }
            else if (obj is EbDataReader)
            {
                Redis.Set(refId, (EbDataReader)obj);
            }
            else if (obj is EbDataWriter)
                Redis.Set(refId, (EbDataWriter)obj);
            else if (obj is EbChart)
            {
                Redis.Set(refId, (EbChart)obj);
            }
            else if (obj is EbTable)
            {
                Redis.Set(refId, (EbTable)obj);
            }
            else if (obj is EbWebForm)
            {
                Redis.Set(refId, (EbWebForm)obj);
            }
            else if (obj is EbUserControl)
            {
                Redis.Set(refId, (EbUserControl)obj);
            }
            else if (obj is EbReport)
            {
                Redis.Set(refId, (EbReport)obj);
            }
            else if (obj is EbBotForm)
            {
                Redis.Set(refId, (EbBotForm)obj);
            }
            else if (obj is EbEmailTemplate)
            {
                Redis.Set(refId, (EbEmailTemplate)obj);
            }
            else if (obj is EbSmsTemplate)
            {
                Redis.Set(refId, (EbSmsTemplate)obj);
            }
            else if (obj is EbSqlFunction)
            {
                Redis.Set(refId, (EbSqlFunction)obj);
            }
            else if (obj is EbApi)
            {
                Redis.Set(refId, (EbApi)obj);
            }
        }

        public string SetAppId(string _apps)
        {
            string appids = string.Empty;
            bool Result = decimal.TryParse(_apps, out decimal myDec);
            if (Result) return myDec.ToString();
            DevRelatedServices myService = base.ResolveService<DevRelatedServices>();
            Dictionary<string, object> res = (myService.Get(new GetApplicationRequest())).Data;
            List<string> applist = _apps.Split(',').ToList();
            foreach (string s in applist)
            {
                foreach (KeyValuePair<string, object> x in res)
                {
                    if (s == Regex.Unescape(x.Value.ToString()).Replace("\n", "").Replace("\t", "").Replace("\r", ""))
                    {
                        appids += x.Key + ",";
                    }
                }
            }
            if (appids == string.Empty)
                appids = "0";
            else
                appids = appids.Substring(0, appids.Length - 1);
            return appids;
        }
    }

    //public class MappedValue
    //{
    //    public EbDbTypes type_ { get; set; }
    //    public object val_ { get; set; }
    //}
}