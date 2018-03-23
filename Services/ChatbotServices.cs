﻿using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.Objects.DVRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    [ClientCanSwapTemplates]
    [Authenticate]
    public class ChatbotServices : EbBaseService
    {
        public ChatbotServices(IEbConnectionFactory _dbf) : base(_dbf) { }


        public GetAppListResponse Get(AppListRequest request)
        {
            string Query1 = @"
                            SELECT
                                    applicationname, application_type, id
                            FROM    
                                    eb_applications;
                        ";
            var table = this.EbConnectionFactory.ObjectsDB.DoQuery(Query1);
            GetAppListResponse resp = new GetAppListResponse();
            foreach (EbDataRow row in table.Rows)
            {
                string appName = row[0].ToString();
                string appType = row[1].ToString();
                string appId = row[2].ToString();
                if (!resp.AppList.ContainsKey(appType))
                {
                    List<string> list = new List<string>();
                    list.Add(appName);
                    list.Add(appId);
                    resp.AppList.Add(appType, list);

                }
                else
                {
                    resp.AppList[appType].Add(appName);
                    resp.AppList[appType].Add(appId);
                }
            }
            //int _id = Convert.ToInt32(request.BotFormIds);
            //var myService = base.ResolveService<EbObjectService>();
            //var res = (EbObjectFetchLiveVersionResponse)myService.Get(new EbObjectFetchLiveVersionRequest() { Id = _id });
            return resp;
        }


        public GetBotDetailsResponse Get(BotDetailsRequest request)
        {
            var Query1 = @"
SELECT 
	name, 
	url, 
	welcome_msg, 
	fullname, 
	botid,
    id
    
FROM 
	eb_bots 
WHERE 
	app_id = @appid;";
            EbDataTable table = this.EbConnectionFactory.ObjectsDB.DoQuery(Query1.Replace("@appid", request.AppId.ToString()));
            GetBotDetailsResponse resp = new GetBotDetailsResponse();
            foreach (EbDataRow row in table.Rows)
            {
                resp.Name = row[0].ToString();
                resp.Url = row[1].ToString();
                resp.WelcomeMsg = row[2].ToString();
                resp.FullName = row[3].ToString();
                resp.BotId = row[4].ToString();
                resp.Id = Convert.ToInt32(row[5]);
            }
            return resp;
        }


        public GetBotForm4UserResponse Get(GetBotForm4UserRequest request)
        {
            /*var Query1 = @"
                            SELECT DISTINCT
		                            EOV.refid, EO.obj_name 
                            FROM
		                            eb_objects EO, eb_objects_ver EOV, eb_objects_status EOS, eb_objects2application EOTA
                            WHERE 
		                            EO.id = EOV.eb_objects_id  AND
		                            EO.id = EOTA.obj_id  AND
		                            EOS.eb_obj_ver_id = EOV.id AND
		                            EO.id =  ANY(@Ids) AND 
		                            EOS.status = 3 AND
		                            ( 	
			                            EO.obj_type = 16 OR
			                            EO.obj_type = 17
			                            OR EO.obj_type = 18
		                            )  AND
		                            EOTA.app_id = @appid AND
                                    EOTA.eb_del = 'F'
                        ";*/
        
            EbDataTable table = this.EbConnectionFactory.ObjectsDB.DoQuery(this.EbConnectionFactory.ObjectsDB.EB_GET_BOT_FORM.Replace("@Ids", request.BotFormIds).Replace("@appid", request.AppId));
            GetBotForm4UserResponse resp = new GetBotForm4UserResponse();
            foreach (EbDataRow row in table.Rows)
            {
                string formRefid = row[0].ToString();
                string formName = row[1].ToString();
                resp.BotForms.Add(formRefid, formName);
            }
            //int _id = Convert.ToInt32(request.BotFormIds);
            //var myService = base.ResolveService<EbObjectService>();
            //var res = (EbObjectFetchLiveVersionResponse)myService.Get(new EbObjectFetchLiveVersionRequest() { Id = _id });
            return resp;
        }

        public CreateBotResponse Post(CreateBotRequest request)
        {
            string botid = null;
            try
            {
                using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    DbCommand cmd = null;
                    string sql = "SELECT * FROM eb_createbot(@solid, @name, @fullname, @url, @welcome_msg, @uid, @botid)";
                    cmd = this.EbConnectionFactory.ObjectsDB.GetNewCommand(con, sql);
                    cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@solid", EbDbTypes.String, request.SolutionId));
                    cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@name", EbDbTypes.String, request.BotName));
                    cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@fullname", EbDbTypes.String, request.FullName));
                    cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@url", EbDbTypes.String, request.WebURL));
                    cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@welcome_msg", EbDbTypes.String, request.WelcomeMsg));
                    cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@uid", EbDbTypes.Int32, request.UserId));
                    cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@botid", EbDbTypes.Int32, (request.BotId != null) ? request.BotId : "0"));

                    botid = cmd.ExecuteScalar().ToString();
                }
            }
            catch (Exception e)
            {

            }
            return new CreateBotResponse() { BotId = botid };
        }

        List<System.Data.Common.DbParameter> parameters = new List<System.Data.Common.DbParameter>();

        [CompressResponse]
        public object Get(BotListRequest request)
        {
            List<ChatBot> res = new List<ChatBot>();
            string sql = @"
SELECT 
    id,
	name, 
	url, 
	botid, 
	(SELECT firstname FROM eb_users WHERE id = eb_bots.created_by) AS created_by, 
	created_at, 
	(SELECT firstname FROM eb_users WHERE id = eb_bots.modified_by) AS modified_by, 
	modified_at, welcome_msg 
FROM 
	eb_bots 
WHERE 
	solution_id = @solid;";
            parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@solid", EbDbTypes.Int32, 100));//request.SolutionId));
            var dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql, parameters.ToArray());

            foreach (var dr in dt.Rows)
            {
                ChatBot bot = new ChatBot
                {
                    BotId = dr[0].ToString(),
                    Name = dr[1].ToString(),
                    WebsiteURL = dr[2].ToString(),
                    ChatId = dr[3].ToString(),
                    CreatedBy = dr[4].ToString(),
                    CreatedAt = Convert.ToDateTime(dr[5]),
                    LastModifiedBy = dr[6].ToString(),
                    LastModifiedAt = Convert.ToDateTime(dr[7]),
                    WelcomeMsg = dr[8].ToString()
                };
                res.Add(bot);
            }
            return new BotListResponse { Data = res };
        }

        public object Any(CreateBotFormTableRequest request)
        {

            //string qry = "SELECT EXISTS (SELECT 1 FROM   information_schema.tables WHERE  table_schema = 'public' AND table_name = @tbl); ";
            //DbParameter[] parameter1 = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("@tbl", EbDbTypes.String, request.BotObj.TableName.ToLower()) };
            DbParameter[] parameter1 = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("tbl", EbDbTypes.String, request.BotObj.TableName.ToLower()) };
            var rslt = this.EbConnectionFactory.ObjectsDB.IsTableExists(this.EbConnectionFactory.ObjectsDB.IS_TABLE_EXIST, parameter1);
            string cols = "";
            var Columns = new DVColumnCollection();
            var pos = 0;
            foreach (EbControl control in request.BotObj.Controls)
            {
                DVBaseColumn _col = null;
                if (control is EbNumeric)
                {
                    //cols += control.Name + " integer,";
                    cols += control.Name + " "+ this.EbConnectionFactory.ObjectsDB.VendorDbTypes.VarNumeric+",";
                    _col = new DVNumericColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.Int32, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
                }
                else if (control is EbTextBox)
                {
                    //cols += control.Name + " text,";
                    cols += control.Name + " " + this.EbConnectionFactory.ObjectsDB.VendorDbTypes.String + ",";
                    _col = new DVStringColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.String, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
                }
                else if (control is EbDate)
                {
                    //cols += control.Name + " Date,";
                    cols += control.Name + " " + this.EbConnectionFactory.ObjectsDB.VendorDbTypes.DateTime + ",";
                    _col = new DVStringColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.DateTime, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
                }
                else if (control is EbInputGeoLocation)
                {
                    //cols += control.Name + " text,";
                    cols += control.Name + " " + this.EbConnectionFactory.ObjectsDB.VendorDbTypes.String + ",";
                    _col = new DVStringColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.String, bVisible = true, sWidth = "100px", ClassName = "dt-body-right tdheight", RenderAs = StringRenderType.Marker};
                }

                //cols = cols.Remove(cols.LastIndexOf(','));
                //cols = cols.Remove(cols.Length-1);

                //EbDbType dbType = (EbDbType)control.GetType().GetPublicStaticField("EbDbType").GetValue(null);
                //cols += control.Name + " " + + ", ";
                Columns.Add(_col);
                pos++;
            }

            if (!rslt)
            {
                string sql = "CREATE TABLE @tbl(@cols)".Replace("@cols", cols.Substring(0, cols.Length - 1).Trim()).Replace("@tbl", request.BotObj.TableName);
                this.EbConnectionFactory.ObjectsDB.CreateTable(sql);
            }
            else { }
            //Alter

            var dsobj = new EbDataSource();
            dsobj.Sql = "SELECT * FROM @tbl".Replace("@tbl", request.BotObj.TableName);
            var ds = new EbObject_Create_New_ObjectRequest();
            ds.Name = request.BotObj.Name+"_datasource";
            ds.Description = "desc";
            ds.Json = EbSerializers.Json_Serialize(dsobj);
            ds.Status = ObjectLifeCycleStatus.Live;
            ds.Relations = "";
            ds.IsSave = false;
            ds.Tags = "";
            ds.Apps = request.Apps;
            ds.TenantAccountId = request.TenantAccountId;
            ds.WhichConsole = request.WhichConsole;
            ds.UserId = request.UserId;
            var myService = base.ResolveService<EbObjectService>();
            var res =myService.Post(ds);
            var refid = res.RefId;

            var dvobj = new EbTableVisualization();
            dvobj.DataSourceRefId = refid;
            dvobj.Columns = Columns;
            dvobj.DSColumns = Columns;
            var ds1 = new EbObject_Create_New_ObjectRequest();
            ds1.Name = request.BotObj.Name + "_response"; 
            ds1.Description = "desc";
            ds1.Json = EbSerializers.Json_Serialize(dvobj);
            ds1.Status = ObjectLifeCycleStatus.Live;
            ds1.Relations = refid;
            ds1.IsSave = false;
            ds1.Tags = "";
            ds1.Apps = request.Apps;
            ds1.TenantAccountId = request.TenantAccountId;
            ds1.WhichConsole = request.WhichConsole;
            ds1.UserId = request.UserId; 
            var res1 = myService.Post(ds1);
            var refid1 = res.RefId;

            return new CreateBotFormTableResponse();
        }

        public object Any(InsertIntoBotFormTableRequest request)
        {
            DbParameter parameter1;
            List<DbParameter> paramlist = new List<DbParameter>();
            string cols = "";
            string vals = "";
            //paramlist.Add(parameter1);
            foreach (var obj in request.Fields)
            {
                cols += obj.Name + ",";
                //if (obj.Value[1] == "integer")
                //{
                //    vals += obj.Value[0] + ",";
                //}
                //else if (obj.Value[1] == "date")
                //{
                //    vals += "'" + this.EbConnectionFactory.ObjectsDB.ConvertToDbDate(obj.Value[0]) + "',";
                //}
                //else
                //    vals += "'" + obj.Value[0] + "',";
                if (obj.Type == "integer")
                {
                    int numreturn;
                    vals += ":"+ obj.Name+",";
                    parameter1 = this.EbConnectionFactory.ObjectsDB.GetNewParameter(obj.Name, EbDbTypes.VarNumeric, double.Parse(obj.Value) );
                }
                else
                {
                   
                    vals += ":" + obj.Name + ",";
                    parameter1 = this.EbConnectionFactory.ObjectsDB.GetNewParameter(obj.Name, EbDbTypes.String, obj.Value);
                }
                paramlist.Add(parameter1);
            }
            cols = cols.Substring(0, cols.Length - 1).Trim();
            vals = vals.Substring(0, vals.Length - 1).Trim();
            var qry = string.Format("insert into @tbl({0}) values({1})".Replace("@tbl",request.TableName), cols, vals);

            var rslt = this.EbConnectionFactory.ObjectsDB.InsertTable(qry,paramlist.ToArray());
            return new InsertIntoBotFormTableResponse();
        }

    }
}
