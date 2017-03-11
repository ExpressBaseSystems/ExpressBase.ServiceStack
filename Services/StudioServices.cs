using System.Collections.Generic;
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

namespace ExpressBase.ServiceStack
{
    [ClientCanSwapTemplates]
    [DefaultView("Form")]
    public class EbObjectService : EbBaseService
    {
        [Authenticate]
        public object Get(EbObjectRequest request)
        {
            var jwtoken = new JwtSecurityToken(request.Token);
            foreach (var c in jwtoken.Claims)
            {
                if (c.Type == "cid")
                {
                    base.ClientID = c.Value;
                    break;
                }
            }

            EbDataTable dt = null;
            using (var con = this.DatabaseFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string _where_clause = (request.Id > 0) ? string.Format("WHERE id={0}", request.Id) : string.Empty;
                dt = this.DatabaseFactory.ObjectsDB.DoQuery(string.Format("SELECT id, obj_name, obj_bytea, obj_type FROM eb_objects {0};", _where_clause));
            };

            List<EbObjectWrapper> f = new List<EbObjectWrapper>();
            foreach (EbDataRow dr in dt.Rows)
            {
                var _form = (new EbObjectWrapper
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = dr[1].ToString(),
                    Bytea = dr[2] as byte[],
                    EbObjectType = (dr[3] == System.DBNull.Value || Convert.ToInt32(dr[3]) == 0) ? EbObjectType.Form : (EbObjectType)Convert.ToInt32(dr[3]),
                });

                f.Add(_form);
            }

            return new EbObjectResponse { Data = f };
        }

        public object Post(EbObjectWrapper request)
        {
            bool result = false;

            using (var con = this.DatabaseFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                DbCommand cmd = null;

                if (request.Id == 0)
                {
                    cmd = this.DatabaseFactory.ObjectsDB.GetNewCommand(con, "INSERT INTO eb_objects (obj_name, obj_bytea, obj_type) VALUES (@obj_name, @obj_bytea, @obj_type);");
                    cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@obj_type", System.Data.DbType.Int32, (int)request.EbObjectType));
                }
                else
                {
                    cmd = this.DatabaseFactory.ObjectsDB.GetNewCommand(con, "UPDATE eb_objects SET obj_name=@obj_name, obj_bytea=@obj_bytea WHERE id=@id;");
                    cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@id", System.Data.DbType.Int32, request.Id));
                }

                cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@obj_name", System.Data.DbType.String, request.Name));
                cmd.Parameters.Add(this.DatabaseFactory.ObjectsDB.GetNewParameter("@obj_bytea", System.Data.DbType.Binary, request.Bytea));

                cmd.ExecuteNonQuery();
                result = true;
            };

            return result;
        }
    }
}

//INSERT INTO eb_objects (obj_name, obj_desc, obj_type) VALUES (@obj_name, @obj_desc, @obj_type);
//INSERT INTO eb_objects_versions(eb_object_id, version, status, submitter_id, submitted_at, obj_bytea, md5_obj_bytea) VALUES(CURRVAL('eb_objects_id_seq'), @version, @status, @submitter_id, @submitted_at, @obj_bytea, @md5_obj_bytea);



