﻿using System.Collections.Generic;
using ServiceStack;
using ServiceStack.DataAnnotations;
using ServiceStack.Text;
using System.Runtime.Serialization;
using ExpressBase.Common;
using ExpressBase.Data;
using System;
using ExpressBase.Objects;
using System.Data.Common;
using System.IdentityModel.Tokens.Jwt;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack.Logging;

namespace ExpressBase.ServiceStack
{
    [ClientCanSwapTemplates]
    [DefaultView("Form")]
    public class EbObjectService : EbBaseService
    {
        [Authenticate]
        public object Get(EbObjectRequest request)
        {
            base.ClientID = request.TenantAccountId;

            EbDataTable dt = null;
            using (var con = this.DatabaseFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string _obj_bytea = (request.Id > 0) ? ", EO.obj_bytea" : string.Empty;
                string _where_clause_part1 = (request.Id > 0) ? string.Format("AND EO.id={0}", request.Id) : string.Empty;

                dt = this.DatabaseFactory.ObjectsDB.DoQuery(string.Format(@"
SELECT 
    EO.id, EO.obj_name, EO.obj_type, EO.obj_last_ver_id, EO.obj_cur_status,EO.obj_desc,
    EOV.id,EOV.eb_objects_id,EOV.ver_num, EOV.obj_changelog,EOV.commit_ts, EOV.commit_ts {0}  
FROM 
    eb_objects EO
INNER JOIN 
    eb_objects_ver EOV
ON 
    EO.id = EOV.eb_objects_id {1}
ORDER BY
    EO.obj_type", _obj_bytea, _where_clause_part1));
            };

            List<EbObjectWrapper> f = new List<EbObjectWrapper>();

            foreach (EbDataRow dr in dt.Rows)
            {
                var _form = (new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString(),
                    EbObjectType = (EbObjectType)Convert.ToInt32(dr[2]),
                    Status = (ObjectLifeCycleStatus)dr[4],
                    Description =dr[5].ToString(),
                    VersionNumber = Convert.ToInt32(dr[8]),
                    Bytea = (request.Id > 0) ? dr[12] as byte[] : null
                });

                f.Add(_form);
            }

            return new EbObjectResponse { Data = f };
        }

        public object Post(EbObjectWrapper request)
        {
            bool result = false;
            base.ClientID = request.TenantAccountId;
            ILog log = LogManager.GetLogger(GetType());
            using (var con = this.DatabaseFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                DbCommand cmd = null;
                log.Info("#DS insert 1 -- con open" );

                if (request.Id > 0)
                {
                    log.Info("#DS insert 1 -- >0"+ request.Id);
                    cmd = this.DatabaseFactory.ObjectsDB.GetNewCommand(con, @"
UPDATE eb_objects SET obj_name=@obj_name,obj_desc=@obj_desc,obj_last_ver_id=(SELECT max(ver_num)+1 FROM eb_objects_ver WHERE eb_objects_id=@id), obj_cur_status=@obj_cur_status WHERE id=@id; 
INSERT INTO eb_objects_ver (eb_objects_id,ver_num,obj_bytea,obj_changelog,commit_uid,commit_ts) VALUES (@id,(SELECT max(ver_num)+1 FROM eb_objects_ver WHERE eb_objects_id=@id),@obj_bytea,@obj_changelog,@commit_uid,now())");
                    cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.Int32, request.Id));
                    cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@obj_changelog", System.Data.DbType.String, request.ChangeLog));
                }
                else
                {
                    log.Info("#DS insert 2 -- !>0");
                    cmd = this.DatabaseFactory.ObjectsDB.GetNewCommand(con, @"
INSERT INTO eb_objects (obj_name,obj_desc,obj_type,obj_last_ver_id,obj_cur_status) VALUES (@obj_name, @obj_desc, @obj_type,1,@obj_cur_status);
INSERT INTO eb_objects_ver (eb_objects_id,ver_num, obj_bytea,commit_uid,commit_ts) VALUES (currval('eb_objects_id_seq'),1,@obj_bytea,@commit_uid,now())");
                    cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@obj_type", System.Data.DbType.Int32, (int)request.EbObjectType));
                }
                cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@obj_name", System.Data.DbType.String, request.Name));
                cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@obj_bytea", System.Data.DbType.Binary, request.Bytea));
                cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@obj_cur_status", System.Data.DbType.Int32, request.Status));
                cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@commit_uid", System.Data.DbType.Int32, request.UserId));
                log.Info("#DS insert 2 -- before exec cmd");
                cmd.ExecuteNonQuery();
                result = true;
            };

            return result;
        }
    }
}

//INSERT INTO eb_objects (obj_name, obj_desc, obj_type) VALUES (@obj_name, @obj_desc, @obj_type);
//INSERT INTO eb_objects_versions(eb_object_id, version, status, submitter_id, submitted_at, obj_bytea, md5_obj_bytea) VALUES(CURRVAL('eb_objects_id_seq'), @version, @status, @submitter_id, @submitted_at, @obj_bytea, @md5_obj_bytea);



