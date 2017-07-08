using System.Collections.Generic;
using ServiceStack;
using ExpressBase.Data;
using System;
using ExpressBase.Objects;
using System.Data.Common;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack.Logging;

namespace ExpressBase.ServiceStack
{
    [ClientCanSwapTemplates]
    [DefaultView("Form")]
    public class EbObjectService : EbBaseService
    {
        private const string Query1 = @"
SELECT 
    EOV.id, EOV.ver_num, EOV.obj_changelog, EOV.commit_ts, EU.firstname
FROM 
    eb_objects_ver EOV, eb_users EU
WHERE
    EOV.commit_uid = EU.id AND
    EOV.eb_objects_id=@id
ORDER BY
    ver_num DESC";

        private const string Query2 = "SELECT obj_bytea FROM eb_objects_ver WHERE id=@id";

        private const string Query3 = @"
SELECT 
    EO.id, EO.obj_name, EO.obj_type, EO.obj_last_ver_id, EO.obj_cur_status,EO.obj_desc,
    EOV.id,EOV.eb_objects_id,EOV.ver_num, EOV.obj_changelog,EOV.commit_ts, EOV.commit_uid, EOV.obj_bytea
FROM 
    eb_objects EO, eb_objects_ver EOV
WHERE
    EO.id = EOV.eb_objects_id AND EO.id=@id AND EO.obj_last_ver_id=EOV.ver_num
ORDER BY
    EO.obj_type";

        private const string Query4 = @"
SELECT 
    EO.id, EO.obj_name, EO.obj_type, EO.obj_last_ver_id, EO.obj_cur_status,EO.obj_desc,
    EOV.id,EOV.eb_objects_id,EOV.ver_num, EOV.obj_changelog,EOV.commit_ts, EOV.commit_uid
FROM 
    eb_objects EO, eb_objects_ver EOV
WHERE
    EO.id = EOV.eb_objects_id AND EO.id=@id AND EO.obj_last_ver_id=EOV.ver_num AND
    EO.obj_type=@type
ORDER BY
    EO.obj_type";

        [Authenticate]
        [CompressResponse]
        public object Get(EbObjectRequest request)
        {
            base.ClientID = request.TenantAccountId;

            List<EbObjectWrapper> f = new List<EbObjectWrapper>();
            ILog log = LogManager.GetLogger(GetType());
            List<System.Data.Common.DbParameter> parameters = new List<System.Data.Common.DbParameter>();

            // Fetch all version without bytea
            if (request.Id > 0 && request.VersionId < Int32.MaxValue)
            {             
                parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.Int32, request.Id));
                var dt = this.DatabaseFactory.ObjectsDB.DoQuery(Query1, parameters.ToArray());

                foreach (EbDataRow dr in dt.Rows)
                {
                    var _ebObject = (new EbObjectWrapper
                    {
                        Id = Convert.ToInt32(dr[0]),
                        VersionNumber = Convert.ToInt32(dr[1]),
                        ChangeLog = dr[2].ToString(),
                        CommitTs =Convert.ToDateTime(dr[3]),
                        CommitUname=dr[4].ToString()
                    });
                    f.Add(_ebObject);
                }
            }

            // Fetch particular version with Bytea
            if (request.VersionId > 0 && request.VersionId < Int32.MaxValue)
            {
                parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.Int32, request.VersionId));
                var dt = this.DatabaseFactory.ObjectsDB.DoQuery(Query1, parameters.ToArray());
           
                foreach (EbDataRow dr in dt.Rows)
                {
                    var _ebObject = (new EbObjectWrapper
                    {
                        Bytea = dr[0] as byte[] 
                    });
                    f.Add(_ebObject);
                }
            }

            // Fetch latest version with bytea
            if (request.Id > 0 && request.VersionId == Int32.MaxValue)
            {
                parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.Int32, request.Id));
                var dt = this.DatabaseFactory.ObjectsDB.DoQuery(Query3, parameters.ToArray());

                foreach (EbDataRow dr in dt.Rows)
                {
                    var _ebObject = (new EbObjectWrapper
                    {
                        Id = Convert.ToInt32(dr[0]),
                        Name = dr[1].ToString(),
                        EbObjectType = (EbObjectType)Convert.ToInt32(dr[2]),
                        Status = (ObjectLifeCycleStatus)dr[4],
                        Description = dr[5].ToString(),
                        VersionNumber = Convert.ToInt32(dr[8]),
                        Bytea = (request.Id > 0) ? dr[12] as byte[] : null
                    });

                    f.Add(_ebObject);
                }
            }

            // Get All latest of this Object Type without Bytea
            if (request.Id == 0 && request.VersionId == Int32.MaxValue)
            {
                parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.Int32, request.Id));
                parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@type", System.Data.DbType.Int32, request.EbObjectType));
                var dt = this.DatabaseFactory.ObjectsDB.DoQuery(Query4, parameters.ToArray());

                foreach (EbDataRow dr in dt.Rows)
                {
                    var _ebObject = (new EbObjectWrapper
                    {
                        Id = Convert.ToInt32(dr[0]),
                        Name = dr[1].ToString(),
                        EbObjectType = (EbObjectType)Convert.ToInt32(dr[2]),
                        Status = (ObjectLifeCycleStatus)dr[4],
                        Description = dr[5].ToString(),
                        VersionNumber = Convert.ToInt32(dr[8])
                    });

                    f.Add(_ebObject);
                }
            }

            return new EbObjectResponse { Data = f };
        }

        [Authenticate]
        public EbObjectWrapperResponse Post(EbObjectWrapper request)
        {
            ILog log = LogManager.GetLogger(GetType());
            log.Info("#DS insert -- entered post");
            bool result = false;
            base.ClientID = request.TenantAccountId;
            EbObjectWrapperResponse res = new EbObjectWrapperResponse();
            using (var con = this.DatabaseFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                DbCommand cmd = null;
                log.Info("#DS insert 1 -- con open");
                if (request.IsSave == "true")
                {
                    log.Info("#DS insert -- con issave true");
                    cmd = this.DatabaseFactory.ObjectsDB.GetNewCommand(con, @"
UPDATE eb_objects SET  obj_name=@obj_name, obj_desc=@obj_desc WHERE id=@id;
UPDATE eb_objects_ver SET obj_bytea= @obj_bytea WHERE eb_objects_id = @id AND ver_num = @ver_num;");
                    cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@ver_num", System.Data.DbType.Int32, request.VersionNumber));
                }
                else
                {
                    if (request.Id > 0)
                    {
                        log.Info("#DS insert 1 -- >0" + request.Id);
                        cmd = this.DatabaseFactory.ObjectsDB.GetNewCommand(con, @"
UPDATE eb_objects SET obj_name=@obj_name,obj_desc=@obj_desc,obj_last_ver_id=(SELECT max(ver_num)+1 FROM eb_objects_ver WHERE eb_objects_id=@id), obj_cur_status=@obj_cur_status WHERE id=@id; 
INSERT INTO eb_objects_ver (eb_objects_id,ver_num,obj_bytea,obj_changelog,commit_uid,commit_ts) VALUES (@id,(SELECT max(ver_num)+1 FROM eb_objects_ver WHERE eb_objects_id=@id),@obj_bytea,@obj_changelog,@commit_uid,now())");
                        cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@obj_changelog", System.Data.DbType.String, request.ChangeLog));
                    }
                    else
                    {
                        log.Info("#DS insert 2 -- !>0");
                        cmd = this.DatabaseFactory.ObjectsDB.GetNewCommand(con, @"
INSERT INTO eb_objects (obj_name,obj_desc,obj_type,obj_last_ver_id,obj_cur_status) VALUES (@obj_name, @obj_desc, @obj_type,1,@obj_cur_status) RETURNING id;
INSERT INTO eb_objects_ver (eb_objects_id,ver_num, obj_bytea,commit_uid,commit_ts) VALUES (currval('eb_objects_id_seq'),1,@obj_bytea,@commit_uid,now())");
                        cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@obj_type", System.Data.DbType.Int32, (int)request.EbObjectType));
                    }
                }
                cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.Int32, request.Id));
                cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@obj_name", System.Data.DbType.String, request.Name));
                cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@obj_desc", System.Data.DbType.String, request.Description));
                cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@obj_bytea", System.Data.DbType.Binary, request.Bytea));
                cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@obj_cur_status", System.Data.DbType.Int32, request.Status));
                cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@commit_uid", System.Data.DbType.Int32, request.UserId));
                log.Info("#DS insert 2 -- before exec cmd");


                res.id = Convert.ToInt32(cmd.ExecuteScalar());

                return res;
            };

            // return res;
        }
    }
}

//INSERT INTO eb_objects (obj_name, obj_desc, obj_type) VALUES (@obj_name, @obj_desc, @obj_type);
//INSERT INTO eb_objects_versions(eb_object_id, version, status, submitter_id, submitted_at, obj_bytea, md5_obj_bytea) VALUES(CURRVAL('eb_objects_id_seq'), @version, @status, @submitter_id, @submitted_at, @obj_bytea, @md5_obj_bytea);



