﻿using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.LocationNSolution;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Security;
using Newtonsoft.Json;
using ServiceStack;
using ServiceStack.Auth;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Net;

namespace ExpressBase.ServiceStack.Services
{
    public class TenantUserServices : EbBaseService
    {
        public TenantUserServices(IEbConnectionFactory _dbf) : base(_dbf) { }

        //public CreateLocationConfigResponse Post(CreateLocationConfigRequest request)
        //{
        //    using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //    {
        //        con.Open();
        //        List<EbLocationConfig> list = request.ConfString;
        //        StringBuilder query1 = new StringBuilder();
        //        query1.Append(@"INSERT INTO eb_location_config (keys,isrequired,keytype,eb_del) VALUES");
        //        List<DbParameter> parameters1 = new List<DbParameter>();
        //        string keys = ":key", isrequired = ":isrequired", type = ":type";
        //        int count = 0;
        //        int InsertCount = 0;
        //        for (int i = 0; i < list.Count(); i++)
        //            if (list[i].KeyId == null)
        //            {
        //                query1.Append("( " + (keys + count) + "," + (isrequired + count) + ","+ (type + count)+",'F'),");
        //                parameters1.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(":key" + count, EbDbTypes.String, list[i].Name));
        //                parameters1.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(":isrequired" + count, EbDbTypes.String, list[i].Isrequired));
        //                parameters1.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(":type" + count, EbDbTypes.String, list[i].Type));
        //                count++;
        //                list.Remove(list[i]);
        //                i--;
        //                InsertCount++;
        //            }
        //        query1.Length--;
        //        query1.Append(";");
        //        int dt1 = 0;
        //        if (InsertCount > 0)
        //            dt1 = this.EbConnectionFactory.ObjectsDB.DoNonQuery(query1.ToString(), parameters1.ToArray());
        //        if (list.Count() == 0)
        //            return new CreateLocationConfigResponse { };
        //        StringBuilder query2 = new StringBuilder();
        //        query2.Append(@"UPDATE eb_location_config AS EL SET keys = L.keys , isrequired =L.isrequired FROM (VALUES");
        //        List<DbParameter> parameters2 = new List<DbParameter>();
        //        string kname = ":kname", kreq = ":kreq", kid = ":kid",ktype = ":ktype";
        //        count = 0;
        //        foreach (var obj in list)
        //        {
        //            query2.Append("(" + (kname + count) + "," + (kreq + count) + "," + (kid + count) +","+ (ktype + count) + "),");
        //            parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter((kname + count), EbDbTypes.String, obj.Name));
        //            parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter((kreq + count), EbDbTypes.String, obj.Isrequired));
        //            parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter((kid + count), EbDbTypes.Int32, Convert.ToInt32(obj.KeyId)));
        //            parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter((ktype + count), EbDbTypes.String, obj.Type));
        //            count++;
        //        }
        //        query2.Length--;
        //        query2.Append(") AS L(keys, isrequired,kid,ktype) WHERE L.kid = EL.id;");
        //        var dt2 = this.EbConnectionFactory.ObjectsDB.DoNonQuery(query2.ToString(), parameters2.ToArray());
        //        return new CreateLocationConfigResponse { };
        //    }
        //}

        [Authenticate]
        public CreateLocationConfigResponse Post(CreateLocationConfigRequest request)
        {
            EbLocationCustomField conf = request.Conf;
            string query = EbConnectionFactory.ObjectsDB.EB_CREATELOCATIONCONFIG1Q;
            string query2 = EbConnectionFactory.ObjectsDB.EB_CREATELOCATIONCONFIG2Q;
            string exeq = "";

            List<DbParameter> parameters = new List<DbParameter>();
            parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("keys", EbDbTypes.String, conf.DisplayName));
            parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("isrequired", EbDbTypes.Boolean, conf.IsRequired));
            parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("type", EbDbTypes.String, conf.Type));

            if (conf.Id != null)
            {
                exeq = query2;
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("keyid", EbDbTypes.String, conf.Id));
            }
            else
                exeq = query;

            EbDataTable ds = this.EbConnectionFactory.ObjectsDB.DoQuery(exeq, parameters.ToArray());

            return new CreateLocationConfigResponse { Id = Convert.ToInt32(ds.Rows[0][0]) };
        }

        [Authenticate]
        public SaveLocationMetaResponse Post(SaveLocationMetaRequest request)
        {
            List<DbParameter> parameters = new List<DbParameter>();
            int result = 0;
            string query1 = EbConnectionFactory.ObjectsDB.EB_SAVELOCATION;
            string query2 = EbConnectionFactory.ObjectsDB.EB_SAVE_LOCATION_2Q;
            parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("lname", EbDbTypes.String, request.Longname));
            parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("sname", EbDbTypes.String, request.Shortname));
            parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("img", EbDbTypes.String, request.Img));
            parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("meta", EbDbTypes.String, request.ConfMeta));
            if (request.Locid > 0)
            {
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("lid", EbDbTypes.Int32, request.Locid));
                int t = this.EbConnectionFactory.ObjectsDB.DoNonQuery(query2.ToString(), parameters.ToArray());
                result = t;
            }
            else
            {
                EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery(query1.ToString(), parameters.ToArray());
                result = Convert.ToInt32(dt.Rows[0][0]);
            }
            this.Post(new UpdateSolutionObjectRequest() { SolnId = request.SolnId, UserId = request.UserId });
            return new SaveLocationMetaResponse { Id = result };
        }

        [Authenticate]
        public SaveLocationResponse Post(SaveLocationRequest request)
        {
            SaveLocationResponse resp = new SaveLocationResponse();
            try
            {
                string query;
                if (request.Location.LocId > 0)
                    query = $@"UPDATE eb_locations SET longname = @lname, shortname = @sname, image = @img, meta_json = @meta, parent_id = @parentid,
                            is_group = @isgroup, eb_location_types_id = @type_id,  eb_lastmodified_by = @by, eb_lastmodified_at = {this.EbConnectionFactory.DataDB.EB_CURRENT_TIMESTAMP}
                            WHERE id = @lid RETURNING id;";
                else
                    query = $@"INSERT INTO eb_locations(longname,shortname,image,meta_json, parent_id, is_group, eb_location_types_id, eb_created_by, eb_created_at) 
                            VALUES(:lname, :sname, :img, :meta, :parentid, :isgroup, :type_id, :by, {this.EbConnectionFactory.DataDB.EB_CURRENT_TIMESTAMP}) RETURNING id;";
                DbParameter[] parameters = {
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("lname", EbDbTypes.String, request.Location.LongName),
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("sname", EbDbTypes.String, request.Location.ShortName),
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("img", EbDbTypes.String, request.Location.Logo),
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("meta", EbDbTypes.String, (request.Location.Meta != null) ? JsonConvert.SerializeObject(request.Location.Meta) : ""),
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("parentid", EbDbTypes.Int32, request.Location.ParentId),
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("isgroup", EbDbTypes.String, (request.Location.IsGroup) ? 'T' : 'F'),
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("type_id", EbDbTypes.Int32, request.Location.TypeId),
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("lid", EbDbTypes.Int32, request.Location.LocId),
                this.EbConnectionFactory.DataDB.GetNewParameter("by", EbDbTypes.Int32, request.UserId),
                };
                resp.Id = this.EbConnectionFactory.DataDB.ExecuteScalar<Int32>(query, parameters);
                this.Post(new UpdateSolutionObjectRequest() { SolnId = request.SolnId, UserId = request.UserId });
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
            }
            return resp;
        }

        [Authenticate]
        public DeleteLocResponse Post(DeleteLocRequest request)
        {
            List<DbParameter> parameters = new List<DbParameter>();
            string query = EbConnectionFactory.ObjectsDB.EB_DELETE_LOC;//RETURNING id is removed
            parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.Id));
            int dt = this.EbConnectionFactory.ObjectsDB.DoNonQuery(query.ToString(), parameters.ToArray());
            return new DeleteLocResponse { id = (dt == 1) ? request.Id : 0 };
        }

        [Authenticate]
        public LockUnlockFyResponse Post(LockUnlockFyRequest request)
        {
            try
            {
                LockUnlockFyRequestObject obj = JsonConvert.DeserializeObject<LockUnlockFyRequestObject>(request.ReqObject);
                string query = $"SELECT * FROM eb_fin_years_lines WHERE id=ANY('{{{obj.FpIdList.Join()}}}') AND eb_del='F'";
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query);
                bool IsAdd = obj.Action == "lock" || obj.Action == "partial_lock";
                string ColName = obj.Action == "lock" || obj.Action == "unlock" ? "locked_ids" : "partially_locked_ids";

                Eb_Solution sol_Obj = this.Redis.Get<Eb_Solution>(String.Format("solution_{0}", request.SolnId));
                if (sol_Obj == null)
                {
                    return new LockUnlockFyResponse() { Status = (int)HttpStatusCode.InternalServerError, Message = "Soluiton object is null" };
                }

                List<int> locIds;
                if (obj.CurrentLoc == -1)
                {
                    locIds = sol_Obj.Locations.Keys.ToList();
                }
                else
                {
                    locIds = new List<int> { obj.CurrentLoc };
                }

                int st = LockUnlockFy(dt, ColName, locIds, IsAdd, request.UserId);

                if (st > 0)
                {
                    sol_Obj.FinancialYears = GetFinancialYears(request.SolnId);
                    this.Redis.Set<Eb_Solution>(String.Format("solution_{0}", request.SolnId), sol_Obj);
                    return new LockUnlockFyResponse() { Status = (int)HttpStatusCode.OK, Message = $"{obj.Action.ReplaceAll("_", " ").ToTitleCase()} operation is successfull" };
                }

                return new LockUnlockFyResponse { Status = (int)HttpStatusCode.BadRequest, Message = "Nothing updated" };
            }
            catch (Exception ex)
            {
                return new LockUnlockFyResponse() { Status = (int)HttpStatusCode.InternalServerError, Message = ex.Message };
            }
        }

        private int LockUnlockFy(EbDataTable dt, string ColName, List<int> LocIds, bool IsAdd, int UserId)
        {
            int status = 0;
            string query = string.Empty;
            foreach (EbDataRow dr in dt.Rows)
            {
                string lids = dr[ColName].ToString();
                List<int> lidsList = lids.Length > 0 ? lids.Split(',').Select(int.Parse).ToList() : new List<int>();
                bool listChanged = false;
                foreach (int lid in LocIds)
                {
                    if (IsAdd)
                    {
                        if (!lidsList.Contains(lid))
                        {
                            lidsList.Add(lid);
                            listChanged = true;
                        }
                    }
                    else
                    {
                        if (lidsList.Contains(lid))
                        {
                            lidsList.Remove(lid);
                            listChanged = true;
                        }
                    }
                }

                if (listChanged)
                {
                    lidsList.Sort();
                    query += $"UPDATE eb_fin_years_lines SET {ColName}='{lidsList.Join()}', eb_lastmodified_by={UserId}, eb_lastmodified_at={this.EbConnectionFactory.DataDB.EB_CURRENT_TIMESTAMP} WHERE id={dr["id"]};";
                }
            }
            if (!string.IsNullOrWhiteSpace(query))
            {
                status = this.EbConnectionFactory.DataDB.DoNonQuery(query);
            }
            return status;
        }

        [Authenticate]
        public CreateNewFyResponse Post(CreateNewFyRequest request)
        {
            try
            {
                Eb_Solution sol_Obj = this.Redis.Get<Eb_Solution>(String.Format("solution_{0}", request.SolnId));
                if (sol_Obj == null)
                {
                    return new CreateNewFyResponse() { Status = (int)HttpStatusCode.InternalServerError, Message = "Soluiton object is null" };
                }
                string query, op;

                if (Int32.TryParse(request.Id, out int fyId) && fyId > 0)
                {
                    query = GetUpdateFyQuery(request, fyId);
                    op = "updation";
                }
                else
                {
                    query = GetInsertFyQuery(request);
                    op = "creation";
                }

                int status = this.EbConnectionFactory.DataDB.DoNonQuery(query);

                if (status > 0)
                {
                    sol_Obj.FinancialYears = GetFinancialYears(request.SolnId);
                    this.Redis.Set<Eb_Solution>(String.Format("solution_{0}", request.SolnId), sol_Obj);
                    return new CreateNewFyResponse() { Status = (int)HttpStatusCode.OK, Message = $"Financial Year {op} successfull" };
                }
                else
                {
                    return new CreateNewFyResponse() { Status = (int)HttpStatusCode.BadRequest, Message = $"Financial Year {op} failed. Please try again later." };
                }
            }
            catch (Exception ex)
            {
                return new CreateNewFyResponse() { Status = (int)HttpStatusCode.InternalServerError, Message = ex.Message };
            }
        }

        private string GetFinYearLinesInsertQry(string duration, DateTime nw_fy_start, DateTime nw_fy_end, int UserId, int FyId)
        {
            string query = string.Empty;
            int Months = 12;
            if (!string.IsNullOrWhiteSpace(duration))
            {
                duration = duration.Trim().Replace(" ", string.Empty).ToLower();
                if (duration.ToLower() == "monthly")
                    Months = 1;
                else if (duration.ToLower() == "quarterly")
                    Months = 3;
                else if (duration.ToLower() == "halfyearly")
                    Months = 6;
            }
            DateTime nw_fp_start = nw_fy_start, nw_fp_end = nw_fp_start.AddMonths(Months).AddDays(-1);

            while (nw_fp_end <= nw_fy_end)
            {
                query += string.Format("INSERT INTO eb_fin_years_lines (active_start, active_end, eb_created_by, eb_created_at, eb_del, eb_fin_years_id) VALUES ('{0}', '{1}', {2}, {3}, 'F', {4});",
                    nw_fp_start.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                    nw_fp_end.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                    UserId,
                    this.EbConnectionFactory.DataDB.EB_CURRENT_TIMESTAMP,
                    FyId > 0 ? FyId.ToString() : "(SELECT eb_currval('eb_fin_years_id_seq'))");
                nw_fp_start = nw_fp_end.AddDays(1);
                nw_fp_end = nw_fp_start.AddMonths(Months).AddDays(-1);
            }
            return query;
        }

        private string GetInsertFyQuery(CreateNewFyRequest request)
        {
            DateTime nw_fy_start, nw_fy_end;
            string query = $"SELECT MAX(fy_end) FROM eb_fin_years WHERE COALESCE(eb_del,'F')='F';";
            EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query);
            if (!dt.Rows[0].IsDBNull(0))
            {
                DateTime fy_end = Convert.ToDateTime(dt.Rows[0][0]).Date;
                nw_fy_start = fy_end.AddDays(1);
            }
            else
            {
                nw_fy_start = DateTime.ParseExact(request.Start, "yyyy-MM-dd", CultureInfo.InvariantCulture);
            }

            nw_fy_end = nw_fy_start.AddYears(1).AddDays(-1);

            query = string.Format("INSERT INTO eb_fin_years (fy_start, fy_end, eb_created_by, eb_created_at, eb_del) VALUES ('{0}', '{1}', {2}, {3}, 'F');",
                nw_fy_start.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                nw_fy_end.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                request.UserId,
                this.EbConnectionFactory.DataDB.EB_CURRENT_TIMESTAMP);

            query += GetFinYearLinesInsertQry(request.Duration, nw_fy_start, nw_fy_end, request.UserId, 0);

            return query;
        }

        private string GetUpdateFyQuery(CreateNewFyRequest request, int fyId)
        {
            string query = $"SELECT fy_start FROM eb_fin_years WHERE id={fyId} AND COALESCE(eb_del,'F')='F';";
            EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query);
            DateTime nw_fy_start, nw_fy_end;
            if (dt.Rows.Count > 0)
            {
                nw_fy_start = Convert.ToDateTime(dt.Rows[0][0]).Date;
                nw_fy_end = nw_fy_start.AddYears(1).AddDays(-1);
            }
            else
            {
                throw new Exception("Financial Year update failed. Start date not found.");
            }

            query = string.Format("UPDATE eb_fin_years_lines SET eb_del='T', eb_lastmodified_by={0}, eb_lastmodified_at={1} WHERE eb_fin_years_id={2} AND eb_del='F';" +
                "UPDATE eb_fin_years SET eb_lastmodified_by={0}, eb_lastmodified_at={1} WHERE id={2} AND eb_del='F';",
                request.UserId, this.EbConnectionFactory.DataDB.EB_CURRENT_TIMESTAMP, fyId);

            query += GetFinYearLinesInsertQry(request.Duration, nw_fy_start, nw_fy_end, request.UserId, fyId);

            return query;
        }

        [Authenticate]
        public UpdateSolutionObjectResponse Post(UpdateSolutionObjectRequest req)
        {
            try
            {
                var _infraService = base.ResolveService<InfraServices>();
                GetSolutioInfoResponse res = (GetSolutioInfoResponse)_infraService.Get(new GetSolutioInfoRequest { IsolutionId = req.SolnId });
                EbSolutionsWrapper wrap_sol = res?.Data;
                LocationInfoTenantResponse Loc = this.Get(new LocationInfoTenantRequest { SolnId = req.SolnId, UserId = req.UserId });
                EbSolutionUsers users = GetUserInfo(req.SolnId);
                if (!(wrap_sol is null))
                {
                    Eb_Solution sol_Obj = new Eb_Solution
                    {
                        SolutionID = req.SolnId,
                        DateCreated = wrap_sol.DateCreated.ToString(),
                        Description = wrap_sol.Description.ToString(),
                        Locations = Loc.Locations,
                        NumberOfUsers = users.UserCount,
                        SolutionName = wrap_sol.SolutionName.ToString(),
                        LocationConfig = Loc.Config,
                        PricingTier = wrap_sol.PricingTier,
                        Users = users.UserList,
                        IsVersioningEnabled = wrap_sol.IsVersioningEnabled,
                        PlanUserCount = users.PlanUserCount,
                        SolutionSettings = wrap_sol.SolutionSettings,
                        ExtSolutionID = wrap_sol.EsolutionId,
                        Is2faEnabled = wrap_sol.Is2faEnabled,
                        OtpDelivery2fa = wrap_sol.OtpDelivery2fa,
                        IsOtpSigninEnabled = wrap_sol.IsOtpSigninEnabled,
                        IsMultiLanguageEnabled = wrap_sol.IsMultiLanguageEnabled,
                        OtpDeliverySignin = wrap_sol.OtpDeliverySignin,
                        SolutionType = wrap_sol.SolutionType,
                        PrimarySolution = wrap_sol.PrimarySolution,
                        FinancialYears = GetFinancialYears(req.SolnId),
                        Languages = GetLanguages(req.SolnId)
                        //LocationTree = Loc.LocationTree
                    };

                    EbConnectionFactory _ebConnectionFactory = new EbConnectionFactory(req.SolnId, Redis);
                    if (_ebConnectionFactory?.EmailConnection?.Primary != null)
                        sol_Obj.IsEmailIntegrated = true;
                    if (_ebConnectionFactory?.SMSConnection?.Primary != null)
                        sol_Obj.IsSmsIntegrated = true;

                    this.Redis.Set<Eb_Solution>(String.Format("solution_{0}", req.SolnId), sol_Obj);
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("sol_Obj Updated : " + sol_Obj.ToString());
                    Console.ForegroundColor = ConsoleColor.White;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error UpdateSolutionRequest: " + e.Message + e.StackTrace);
            }
            return new UpdateSolutionObjectResponse { };
        }

        public UpdateUserObjectResponse Post(UpdateUserObjectRequest request)
        {
            try
            {
                User user = null;
                IEbConnectionFactory factory = new EbConnectionFactory(request.SolnId, Redis);
                if (factory != null && factory.DataDB != null)
                {
                    user = User.GetUserObject(factory.DataDB, request.UserId, request.WC, request.UserIp, request.DeviceId);
                    user.AuthId = request.UserAuthId;
                    if (request.IsApiUser)
                    {
                        string sql = $"SELECT id FROM eb_user_api_keys WHERE eb_users_id={request.UserId} AND eb_del='F';";
                        EbDataTable dt = factory.DataDB.DoQuery(sql);
                        if (dt.Rows.Count > 0)
                            user.ApiKeyId = Convert.ToInt32(dt.Rows[0][0]);
                    }
                    this.Redis.Set<IUserAuth>(request.UserAuthId, user);
                    Console.ForegroundColor = ConsoleColor.Blue;
                    Console.WriteLine("User Object Updated : " + request.UserAuthId);
                    Console.ForegroundColor = ConsoleColor.White;
                }
                else { Console.WriteLine("Connectionfactory not available frm redis" + request.SolnId); }

            }
            catch (Exception e)
            {
                Console.WriteLine("Error UpdateUserObjectRequest: " + e.Message + e.StackTrace);
            }
            return new UpdateUserObjectResponse { };
        }

        public EbSolutionUsers GetUserInfo(string solnId)
        {
            EbSolutionUsers SolutionUsers = new EbSolutionUsers();
            try
            {
                string query = string.Format(@"SELECT user_no FROM eb_subscription WHERE 
                                cust_id = (select cust_id from eb_customer where solution_id = '{0}')", solnId);
                EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(query);

                SolutionUsers.PlanUserCount = (dt.Rows.Count > 0) ? (int)dt.Rows[0]["user_no"] : 5; /// Hardcoding 5

                EbConnectionFactory _ebConnectionFactory = new EbConnectionFactory(solnId, this.Redis);
                string sql = @"SELECT COUNT(*) FROM eb_users WHERE (statusid = 0 OR statusid = 1 OR statusid = 2) AND eb_del ='F';
                        SELECT id, fullname from eb_users; ";  // statusid 0 - active users, 1- suspended users
                EbDataSet ds = _ebConnectionFactory.DataDB.DoQueries(sql);
                if (ds.Tables != null)
                {
                    if (ds.Tables[0] != null && ds.Tables[0].Rows.Count > 0)
                        SolutionUsers.UserCount = Convert.ToInt32(ds.Tables[0].Rows[0][0]);

                    if (ds.Tables[1] != null && ds.Tables[1].Rows.Count > 0)
                    {
                        SolutionUsers.UserList = new Dictionary<int, string>();
                        foreach (EbDataRow r in ds.Tables[1].Rows)
                            if (!SolutionUsers.UserList.ContainsKey((int)r[0]))
                                SolutionUsers.UserList[(int)r[0]] = r[1].ToString();
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
                throw e;
            }
            return SolutionUsers;
        }

        private EbFinancialYears GetFinancialYears(string solnId)
        {
            EbFinancialYears FinYears = new EbFinancialYears();
            try
            {
                EbConnectionFactory _ebConFactory = new EbConnectionFactory(solnId, this.Redis);
                string sql = @"
SELECT 
    y.id, y.fy_start, y.fy_end, yl.id AS eb_fin_years_lines_id, yl.active_start, yl.active_end, yl.locked_ids, yl.partially_locked_ids
FROM 
    eb_fin_years y, eb_fin_years_lines yl
WHERE 
    y.id = yl.eb_fin_years_id AND
    y.eb_del = 'F' AND  yl.eb_del = 'F'
ORDER BY 
    y.fy_start, yl.active_start;";

                EbDataTable dt = _ebConFactory.DataDB.DoQuery(sql);
                List<int> _locIds;
                List<int> _locIdsPartial;
                int fyId;
                EbFinancialYear FinY;
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    fyId = Convert.ToInt32(dt.Rows[i][0]);
                    FinY = FinYears.List.Find(e => e.Id == fyId);
                    if (FinY == null)
                    {
                        FinY = new EbFinancialYear()
                        {
                            Id = fyId,
                            FyStart = Convert.ToDateTime(dt.Rows[i][1]).Date,
                            FyEnd = Convert.ToDateTime(dt.Rows[i][2]).Date
                        };
                        FinYears.List.Add(FinY);
                    }

                    _locIds = string.IsNullOrWhiteSpace(Convert.ToString(dt.Rows[i][6])) ? new List<int>() : Convert.ToString(dt.Rows[i][6]).Split(",").Select(Int32.Parse).ToList();
                    _locIdsPartial = string.IsNullOrWhiteSpace(Convert.ToString(dt.Rows[i][7])) ? new List<int>() : Convert.ToString(dt.Rows[i][7]).Split(",").Select(Int32.Parse).ToList();

                    FinY.List.Add(new EbFinancialPeriod()
                    {
                        Id = Convert.ToInt32(dt.Rows[i][3]),
                        FyId = FinY.Id,
                        ActStart = Convert.ToDateTime(dt.Rows[i][4]).Date,
                        ActEnd = Convert.ToDateTime(dt.Rows[i][5]).Date,
                        LockedIds = _locIds,
                        PartiallyLockedIds = _locIdsPartial
                    });

                }
                //if (dt.Rows.Count > 0)
                //    FinYears.Current = Convert.ToInt32(dt.Rows[0][0]);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error in GetFinancialYears: {e.Message}\n{e.StackTrace}");
            }
            return FinYears;
        }

        private List<EbLanguage> GetLanguages(string solnId)
        {
            List<EbLanguage> list = new List<EbLanguage>();
            try
            {
                EbConnectionFactory _ebConFactory = new EbConnectionFactory(solnId, this.Redis);

                string sql = @"
SELECT 
    l.id, l.code, l.name, l.display_name
FROM 
    eb_languages l
WHERE 
    COALESCE(l.eb_del, 'F') = 'F'
ORDER BY 
    l.eb_row_num;";

                EbDataTable dt = _ebConFactory.DataDB.DoQuery(sql);
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    list.Add(new EbLanguage()
                    {
                        Id = Convert.ToInt32(dt.Rows[i][0]),
                        Code = Convert.ToString(dt.Rows[i][1]),
                        Name = Convert.ToString(dt.Rows[i][2]),
                        DisplayName = Convert.ToString(dt.Rows[i][3])
                    });
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error in GetLanguages: {e.Message}\n{e.StackTrace}");
            }
            return list;
        }

        [Authenticate]
        public LocationInfoTenantResponse Get(LocationInfoTenantRequest req)
        {
            List<EbLocationCustomField> Conf = new List<EbLocationCustomField>();
            Dictionary<int, EbLocation> locs = new Dictionary<int, EbLocation>();
            Dictionary<int, EbLocation> loctree = new Dictionary<int, EbLocation>();
            try
            {
                string query = @"SELECT
                                        * 
                                FROM 
                                    eb_location_config 
                                WHERE 
                                    eb_del = 'F' 
                                ORDER BY id;

                                SELECT 
                                    L.id, L.shortname, L.longname, L.image, L.meta_json, L.week_holiday1, L.week_holiday2, L.is_group, L.parent_id, T.id, T.type
                                FROM 
                                    eb_locations L
                                LEFT JOIN 
                                    eb_location_types T
                                ON 
                                    L.eb_location_types_id = T.id
                                WHERE 
                                    COALESCE(L.eb_del,'F') = 'F'
                                ORDER BY parent_id";
                EbConnectionFactory ebConnectionFactory = new EbConnectionFactory(req.SolnId.ToLower(), this.Redis);
                EbDataSet dt = ebConnectionFactory.DataDB.DoQueries(query);
                if (dt != null && dt.Tables.Count > 0)
                {
                    foreach (EbDataRow r in dt.Tables[0].Rows)
                    {
                        Conf.Add(new EbLocationCustomField
                        {
                            Name = r[1].ToString(),
                            IsRequired = (r[2].ToString() == "T") ? true : false,
                            Id = r[0].ToString(),
                            Type = r[3].ToString()
                        });
                    }

                    foreach (EbDataRow r in dt.Tables[1].Rows)
                    {
                        locs.Add(Convert.ToInt32(r[0]), new EbLocation
                        {
                            LocId = Convert.ToInt32(r[0]),
                            ShortName = r[1].ToString(),
                            LongName = r[2].ToString(),
                            Logo = r[3].ToString(),
                            Meta = JsonConvert.DeserializeObject<Dictionary<string, string>>(r[4].ToString()),
                            WeekHoliday1 = r[5].ToString(),
                            WeekHoliday2 = r[6].ToString(),
                            IsGroup = (r[7].ToString() == "T") ? true : false,
                            ParentId = Convert.ToInt32(r[8]),
                            TypeId = Convert.ToInt32(r[9]),
                            TypeName = r[10].ToString()
                        });
                        if (r[10].ToString() == string.Empty)
                        {
                            Console.WriteLine("Location: " + r[2].ToString() + " , Location Type Not Set");
                        }
                    }

                }

                var tree = dt.Tables[1].Enumerate().ToTree(row => true,
                        (parent, child) => Convert.ToInt32(parent["id"]) == Convert.ToInt32(child["parent_id"]), "is_group");
                foreach (Node<EbDataRow> Nodedr in tree.Tree)
                {
                    loctree.Add(Convert.ToInt32(Nodedr.Item["id"]), CreateLocationObject(Nodedr.Item));
                    if (Nodedr.Children.Count > 0)
                        RecursivelyGetChildren(loctree, Nodedr);
                }

            }
            catch (Exception e)
            {
                Console.WriteLine("Erron in Getting location info" + e.Message + e.StackTrace);
            }

            return new LocationInfoTenantResponse { Locations = locs, Config = Conf, LocationTree = loctree };
        }

        [Authenticate]
        public LocationInfoResponse Get(LocationInfoRequest req)
        {
            List<EbLocationCustomField> Conf = new List<EbLocationCustomField>();
            Dictionary<int, EbLocation> locs = new Dictionary<int, EbLocation>();
            List<EbLocationType> locTypes = new List<EbLocationType>();

            string query = @"
                            SELECT * FROM eb_location_config WHERE COALESCE(eb_del,'F')  = 'F' ORDER BY id;
                            SELECT * FROM eb_locations WHERE COALESCE(eb_del,'F') = 'F' ORDER BY id; 
                            SELECT * FROM eb_location_types WHERE COALESCE(eb_del,'F')  = 'F' ORDER BY id";
            EbDataSet dt = this.EbConnectionFactory.ObjectsDB.DoQueries(query);

            foreach (EbDataRow r in dt.Tables[0].Rows)
            {
                Conf.Add(new EbLocationCustomField
                {
                    DisplayName = r[1].ToString(),
                    Name = r[1].ToString().Replace(" ", ""),
                    IsRequired = (r[2].ToString() == "T") ? true : false,
                    Id = r[0].ToString(),
                    Type = r[3].ToString()
                });
            }

            foreach (var r in dt.Tables[1].Rows)
            {
                locs.Add(Convert.ToInt32(r[0]), new EbLocation
                {
                    LocId = Convert.ToInt32(r[0]),
                    ShortName = r[1].ToString(),
                    LongName = r[2].ToString(),
                    Logo = r[3].ToString(),
                    Meta = JsonConvert.DeserializeObject<Dictionary<string, string>>(r[4].ToString())
                });
            }
            foreach (var r in dt.Tables[2].Rows)
            {
                locTypes.Add(new EbLocationType
                {
                    Id = Convert.ToInt32(r[0]),
                    Type = r[1].ToString()
                });
            }
            return new LocationInfoResponse { Locations = locs, Config = Conf, LocationTypes = locTypes };
        }

        [Authenticate]
        public GetUserDashBoardObjectsResponse Any(GetUserDashBoardObjectsRequest request)
        {
            string query = @"SELECT t2.* FROM
                        (
	                        SELECT 
 		                        q.ver_id as ver_id FROM( 
			                        SELECT 
				                        eos.eb_obj_ver_id as ver_id, eos.status as t_status 
			                        FROM 
    			                        eb_objects_status eos WHERE eos.id IN (
					                        SELECT MAX(eos1.id) AS id1 FROM eb_objects_status eos1 WHERE eos1.eb_obj_ver_id IN(
						                        SELECT eov.id FROM eb_objects_ver eov, eb_objects eo 
                                                WHERE
                                                eov.eb_objects_id = eo.id And eo.obj_type = 22
                                                {0}
                                                AND coalesce(eov.eb_del,'F')='F' 
                                                AND coalesce(eo.eb_del,'F')='F' ) 
                                                GROUP BY eos1.eb_obj_ver_id )
				                        )q WHERE t_status=3
                        ) t1
                        LEFT JOIN				
                        (
                        SELECT 
 	                        eov.eb_objects_id, eov.id AS ver_id, eov.refid, eov.obj_json
                        FROM
	                        eb_objects_ver eov
                        )t2
                        ON t1.ver_id = t2.ver_id; ";
            EbDataTable dt = null;
            if (request.SolutionOwner)
            {
                query = string.Format(query, string.Empty);
                dt = EbConnectionFactory.ObjectsDB.DoQuery(query);
            }
            else
            {
                query = string.Format(query, EbConnectionFactory.DataDB.EB_GET_USER_DASHBOARD_OBJECTS);
                DbParameter[] parameters =
                {
                    this.EbConnectionFactory.DataDB.GetNewParameter("ids", EbDbTypes.String, String.Join(",",request.ObjectIds)),
                };
                dt = EbConnectionFactory.ObjectsDB.DoQuery(query, parameters);
            }

            Dictionary<string, EbDashBoard> Wrap = new Dictionary<string, EbDashBoard>();

            if (dt.Rows.Count != 0)
            {
                foreach (EbDataRow dr in dt.Rows)
                {
                    Wrap.Add(dr["refid"].ToString(), EbSerializers.Json_Deserialize<EbDashBoard>(dr["obj_json"].ToString()));

                }
            }

            return new GetUserDashBoardObjectsResponse { DashBoardObjectIds = Wrap };
        }

        //private string GeneratePassword()
        //{
        //    string strPwdchar = "abcdefghijklmnopqrstuvwxyz0123456789#+@&$ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        //    string strPwd = "";
        //    Random rnd = new Random();
        //    for (int i = 0; i <= 7; i++)
        //    {
        //        int iRandom = rnd.Next(0, strPwdchar.Length - 1);
        //        strPwd += strPwdchar.Substring(iRandom, 1);
        //    }
        //    return strPwd;
        //}

        //    public CreateUserResponse Post(CreateUserRequest request)
        //    {
        //        CreateUserResponse resp;
        //        string sql = "";
        //        using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //        {
        //            con.Open();
        //string password = "";

        //            if (request.Id > 0)
        //            {
        //	sql = "SELECT * FROM eb_createormodifyuserandroles(@userid,@id,@firstname,@email,@pwd,@roles,@group);";

        //}
        //            else
        //            {
        //	password = string.IsNullOrEmpty(request.Colvalues["pwd"].ToString()) ? GeneratePassword() : (request.Colvalues["pwd"].ToString() + request.Colvalues["email"].ToString()).ToMD5Hash();
        //	sql = "SELECT * FROM eb_createormodifyuserandroles(@userid,@id,@firstname,@email,@pwd,@roles,@group);";

        //}
        //int[] emptyarr = new int[] { };
        //            DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("firstname", EbDbTypes.String, request.Colvalues["firstname"]),
        //                        this.EbConnectionFactory.ObjectsDB.GetNewParameter("email", EbDbTypes.String, request.Colvalues["email"]),
        //                        this.EbConnectionFactory.ObjectsDB.GetNewParameter("roles", EbDbTypes.String,(request.Colvalues["roles"].ToString() != string.Empty? request.Colvalues["roles"] : string.Empty )),
        //                        this.EbConnectionFactory.ObjectsDB.GetNewParameter("group", EbDbTypes.String,(request.Colvalues["group"].ToString() != string.Empty? request.Colvalues["group"] : string.Empty )),
        //                        this.EbConnectionFactory.ObjectsDB.GetNewParameter("pwd", EbDbTypes.String,password),
        //                        this.EbConnectionFactory.ObjectsDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
        //                        this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.Id)};

        //            EbDataSet dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

        //            if (string.IsNullOrEmpty(request.Colvalues["pwd"].ToString()) && request.Id < 0)
        //            {
        //                using (var service = base.ResolveService<EmailService>())
        //                {
        //                  //  service.Post(new EmailServicesRequest() { To = request.Colvalues["email"].ToString(), Subject = "New User", Message = string.Format("You are invited to join as user. Log in {0}.localhost:53431 using Username: {1} and Password : {2}", request.TenantAccountId, request.Colvalues["email"].ToString(), dt.Tables[0].Rows[0][1]) });
        //                }
        //            }
        //            resp = new CreateUserResponse
        //            {
        //                id = Convert.ToInt32(dt.Tables[0].Rows[0][0])

        //            };
        //        } 
        //        return resp;
        //    }

        //     public GetUserEditResponse Any(GetUserEditRequest request)
        //     {
        //         GetUserEditResponse resp = new GetUserEditResponse();
        //string sql = null;
        //if (request.Id > 0)
        //{
        //	sql = @"SELECT id, role_name, description FROM eb_roles ORDER BY role_name;
        //                     SELECT id, name,description FROM eb_usergroup ORDER BY name;
        //			SELECT firstname,email FROM eb_users WHERE id = @id;
        //			SELECT role_id FROM eb_role2user WHERE user_id = @id AND eb_del = 'F';
        //			SELECT groupid FROM eb_user2usergroup WHERE userid = @id AND eb_del = 'F';";
        //}
        //else
        //{
        //	sql = @"SELECT id, role_name, description FROM eb_roles ORDER BY role_name;
        //                     SELECT id, name,description FROM eb_usergroup ORDER BY name";
        //}

        //DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("@id", EbDbTypes.Int32, request.Id) };
        //var ds = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

        //resp.Roles = new List<EbRole>();
        //foreach (var dr in ds.Tables[0].Rows)
        //{ 						
        //	resp.Roles.Add(new EbRole
        //	{
        //		Id = Convert.ToInt32(dr[0]),
        //		Name = dr[1].ToString(),
        //		Description = dr[2].ToString()
        //	});
        //}

        //resp.EbUserGroups = new List<EbUserGroups>();
        //foreach (var dr in ds.Tables[1].Rows)
        //{
        //	resp.EbUserGroups.Add(new EbUserGroups
        //	{
        //		Id = Convert.ToInt32(dr[0]),
        //		Name = dr[1].ToString(),
        //		Description = dr[2].ToString()

        //	});
        //}

        //if (request.Id > 0)
        //{
        //	resp.UserData = new Dictionary<string, object>();
        //	foreach (var dr in ds.Tables[2].Rows)
        //	{
        //		resp.UserData.Add("name", dr[0].ToString());
        //		resp.UserData.Add("email", dr[1].ToString());
        //	}

        //	resp.UserRoles = new List<int>();
        //	foreach (var dr in ds.Tables[3].Rows)
        //		resp.UserRoles.Add(Convert.ToInt32(dr[0]));

        //	resp.UserGroups = new List<int>();
        //	foreach (var dr in ds.Tables[4].Rows)
        //		resp.UserGroups.Add(Convert.ToInt32(dr[0]));
        //}

        //         return resp;
        //     }

        //     public GetUserRolesResponse Any(GetUserRolesRequest request)
        //     {
        //         GetUserRolesResponse resp = new GetUserRolesResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = string.Empty;
        //             if (request.id > 0)
        //                 sql = @"SELECT id, role_name, description FROM eb_roles;
        //                          SELECT id, role_name, description FROM eb_roles WHERE id IN(SELECT role_id FROM eb_role2user WHERE user_id = @id AND eb_del = 'F')";
        //             else
        //                 sql = "SELECT id,role_name, description FROM eb_roles";

        //             DbParameter[] parameters = {
        //                         this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.id)};

        //             var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

        //             Dictionary<string, object> returndata = new Dictionary<string, object>();
        //             List<int> subroles = new List<int>();
        //             foreach (EbDataRow dr in dt.Tables[0].Rows)
        //             {
        //		List<string> list = new List<string>();
        //		list.Add(dr[1].ToString());
        //		list.Add(dr[2].ToString());
        //                 returndata[dr[0].ToString()] = list;
        //             }

        //             if (dt.Tables.Count > 1)
        //             {
        //                 foreach (EbDataRow dr in dt.Tables[1].Rows)
        //                 {
        //                     subroles.Add(Convert.ToInt32(dr[0]));
        //                 }
        //                 returndata.Add("roles", subroles);
        //             }
        //             resp.Data = returndata;
        //         }
        //         return resp;
        //     }


        //     public RBACRolesResponse Post(RBACRolesRequest request)
        //     {
        //         RBACRolesResponse resp;
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = "SELECT eb_create_or_update_rbac_manageroles(@role_id, @applicationid, @createdby, @role_name, @description, @users, @dependants,@permission );";
        //             var cmd = this.EbConnectionFactory.ObjectsDB.GetNewCommand(con, sql);
        //             int[] emptyarr = new int[] { };
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("role_id", EbDbTypes.Int32, request.Colvalues["roleid"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("description", EbDbTypes.String, request.Colvalues["Description"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("role_name", EbDbTypes.String, request.Colvalues["role_name"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("applicationid", EbDbTypes.Int32, request.Colvalues["applicationid"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby", EbDbTypes.Int32, request.UserId));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("permission", EbDbTypes.String, (request.Colvalues["permission"].ToString() != string.Empty) ? request.Colvalues["permission"] : string.Empty));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("users", EbDbTypes.String, (request.Colvalues["users"].ToString() != string.Empty) ? request.Colvalues["users"] : string.Empty));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("dependants", EbDbTypes.String, (request.Colvalues["dependants"].ToString() != string.Empty) ? request.Colvalues["dependants"] : string.Empty));

        //             resp = new RBACRolesResponse
        //             {
        //                 id = Convert.ToInt32(cmd.ExecuteScalar())

        //             };
        //         }
        //         return resp;
        //     }

        //     public CreateUserGroupResponse Post(CreateUserGroupRequest request)
        //     {
        //         CreateUserGroupResponse resp;
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = "";
        //             if (request.Id > 0)
        //             {
        //                 sql = @"UPDATE eb_usergroup SET name = @name,description = @description WHERE id = @id;
        //                                 INSERT INTO eb_user2usergroup(userid,groupid) SELECT uid,@id FROM UNNEST(array(SELECT unnest(@users) except 
        //                                     SELECT UNNEST(array(SELECT userid from eb_user2usergroup WHERE groupid = @id AND eb_del = 'F')))) as uid;
        //                                 UPDATE eb_user2usergroup SET eb_del = 'T' WHERE userid IN(
        //                                     SELECT UNNEST(array(SELECT userid from eb_user2usergroup WHERE groupid = @id AND eb_del = 'F')) except SELECT UNNEST(@users));";
        //             }
        //             else
        //             {
        //                 sql = @"INSERT INTO eb_usergroup (name,description) VALUES (@name,@description) RETURNING id;
        //                                    INSERT INTO eb_user2usergroup (userid,groupid) SELECT id, (CURRVAL('eb_usergroup_id_seq')) FROM UNNEST(@users) AS id";
        //             }

        //             var cmd = this.EbConnectionFactory.ObjectsDB.GetNewCommand(con, sql);
        //             int[] emptyarr = new int[] { };
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("name", EbDbTypes.String, request.Colvalues["groupname"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("description", EbDbTypes.String, request.Colvalues["description"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("users", EbDbTypes.String, (request.Colvalues["userlist"].ToString() != string.Empty) ? request.Colvalues["userlist"] : string.Empty));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.Id));
        //             resp = new CreateUserGroupResponse
        //             {
        //                 id = Convert.ToInt32(cmd.ExecuteScalar())

        //             };
        //         }
        //         return resp;
        //     } //user group creation

        //     public UserPreferenceResponse Post(UserPreferenceRequest request)
        //     {
        //         UserPreferenceResponse resp;
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             var cmd = this.EbConnectionFactory.ObjectsDB.GetNewCommand(con, "UPDATE eb_users SET locale=@locale,timezone=@timezone,dateformat=@dateformat,numformat=@numformat,timezonefull=@timezonefull WHERE id=@id");
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("locale", EbDbTypes.String, request.Colvalues["locale"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("timezone", EbDbTypes.String, request.Colvalues["timecode"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("dateformat", EbDbTypes.String, request.Colvalues["dateformat"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("numformat", EbDbTypes.String, request.Colvalues["numformat"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("timezonefull", EbDbTypes.String, request.Colvalues["timezone"]));
        //             cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.Colvalues["uid"]));
        //             resp = new UserPreferenceResponse
        //             {
        //                 id = Convert.ToInt32(cmd.ExecuteScalar())

        //             };
        //         }
        //         return resp;
        //     } //adding user preference like timezone

        //     public EditUserPreferenceResponse Post(EditUserPreferenceRequest request)
        //     {
        //         EditUserPreferenceResponse resp = new EditUserPreferenceResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             string sql = "SELECT dateformat,timezone,numformat,timezoneabbre,timezonefull,locale FROM eb_users WHERE id = @id;";
        //             DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("@id", EbDbTypes.Int32, request.UserId) };

        //             var ds = this.EbConnectionFactory.ObjectsDB.DoQuery(sql, parameters);

        //             Dictionary<string, object> result = new Dictionary<string, object>();
        //             if (ds.Rows.Count > 0)
        //             {
        //                 foreach (var dr in ds.Rows)
        //                 {

        //                     result.Add("dateformat", dr[0].ToString());
        //                     result.Add("timezone", dr[1].ToString());
        //                     result.Add("numformat", dr[2].ToString());
        //                     result.Add("timezoneabbre", dr[3].ToString());
        //                     result.Add("timezonefull", dr[4].ToString());
        //                     result.Add("locale", dr[5].ToString());
        //                 }
        //             }
        //             resp.Data = result;
        //         }
        //         return resp;
        //     }

        //     public GetSubRolesResponse Any(GetSubRolesRequest request)
        //     {
        //         GetSubRolesResponse resp = new GetSubRolesResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = string.Empty;
        //             if (request.id > 0)
        //                 sql = @"
        //                                SELECT id,role_name FROM eb_roles WHERE id != @id AND applicationid= @applicationid;
        //                                SELECT role2_id FROM eb_role2role WHERE role1_id = @id AND eb_del = 'F'";
        //             else
        //                 sql = "SELECT id,role_name FROM eb_roles WHERE applicationid= @applicationid";

        //             DbParameter[] parameters = {
        //                         this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.id),
        //                     this.EbConnectionFactory.ObjectsDB.GetNewParameter("applicationid", EbDbTypes.Int32,request.Colvalues["applicationid"])};

        //             var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

        //             Dictionary<string, object> returndata = new Dictionary<string, object>();
        //             List<int> subroles = new List<int>();
        //             foreach (EbDataRow dr in dt.Tables[0].Rows)
        //             {
        //                 returndata[dr[0].ToString()] = dr[1].ToString();
        //             }

        //             if (dt.Tables.Count > 1)
        //             {
        //                 foreach (EbDataRow dr in dt.Tables[1].Rows)
        //                 {
        //                     subroles.Add(Convert.ToInt32(dr[0]));
        //                 }
        //                 returndata.Add("roles", subroles);
        //             }
        //             resp.Data = returndata;
        //         }
        //         return resp;
        //     }

        //     public GetRolesResponse Any(GetRolesRequest request)
        //     {
        //         GetRolesResponse resp = new GetRolesResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = "SELECT id,role_name FROM eb_roles";
        //             var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql);

        //             Dictionary<string, object> returndata = new Dictionary<string, object>();
        //             foreach (EbDataRow dr in dt.Tables[0].Rows)
        //             {
        //                 returndata[dr[0].ToString()] = dr[1].ToString();
        //             }
        //             resp.Data = returndata;
        //         }
        //         return resp;
        //     }

        //     public GetPermissionsResponse Any(GetPermissionsRequest request)
        //     {
        //         GetPermissionsResponse resp = new GetPermissionsResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = @"
        //             SELECT role_name,applicationid,description FROM eb_roles WHERE id = @id;
        //             SELECT permissionname,obj_id,op_id FROM eb_role2permission WHERE role_id = @id AND eb_del = 'F';
        //             SELECT applicationname FROM eb_applications WHERE id IN(SELECT applicationid FROM eb_roles WHERE id = @id);";



        //             DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.id) };

        //             var ds = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);
        //             List<string> _lstPermissions = new List<string>();

        //             foreach (var dr in ds.Tables[1].Rows)
        //                 _lstPermissions.Add(dr[0].ToString());

        //             resp.Permissions = _lstPermissions;
        //             Dictionary<string, object> result = new Dictionary<string, object>();
        //             foreach (var dr in ds.Tables[0].Rows)
        //             {

        //                 result.Add("rolename", dr[0].ToString());
        //                 result.Add("applicationid", Convert.ToInt32(dr[1]));
        //                 result.Add("description", dr[2].ToString());
        //             }
        //             foreach (var dr in ds.Tables[2].Rows)
        //                 result.Add("applicationname", dr[0].ToString());

        //             resp.Data = result;
        //         }
        //         return resp;
        //     } // for getting saved permissions

        //     public GetUsersResponse Any(GetUsersRequest request)
        //     {
        //         GetUsersResponse resp = new GetUsersResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = "SELECT id,firstname FROM eb_users WHERE firstname ~* @searchtext";

        //             DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("searchtext", EbDbTypes.String, (request.Colvalues != null) ? request.Colvalues["searchtext"] : string.Empty) };

        //             var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

        //             Dictionary<string, object> returndata = new Dictionary<string, object>();
        //             foreach (EbDataRow dr in dt.Tables[0].Rows)
        //             {
        //                 returndata[dr[0].ToString()] = dr[1].ToString();
        //             }
        //             resp.Data = returndata;
        //         }
        //         return resp;
        //     } //for user search

        //     public GetUsersRoleResponse Any(GetUsersRoleRequest request)
        //     {
        //         GetUsersRoleResponse resp = new GetUsersRoleResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = @"SELECT id,firstname FROM eb_users WHERE id IN(SELECT user_id FROM eb_role2user WHERE role_id = @roleid AND eb_del = 'F')";


        //             DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("roleid", EbDbTypes.Int32, request.id) };

        //             var dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql, parameters);

        //             Dictionary<string, object> returndata = new Dictionary<string, object>();

        //             foreach (EbDataRow dr in dt.Rows)
        //             {
        //                 returndata[dr[0].ToString()] = dr[1].ToString();
        //             }
        //             resp.Data = returndata;
        //         }
        //         return resp;
        //     }

        //     public GetUser2UserGroupResponse Any(GetUser2UserGroupRequest request)
        //     {
        //         GetUser2UserGroupResponse resp = new GetUser2UserGroupResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql;
        //             if (request.id > 0)
        //             {
        //                 sql = @"SELECT id, name FROM eb_usergroup;
        //                         SELECT id,name FROM eb_usergroup WHERE id IN(SELECT groupid FROM eb_user2usergroup WHERE userid = @userid AND eb_del = 'F')";
        //             }
        //             else
        //             {
        //                 sql = "SELECT id, name FROM eb_usergroup";
        //             }

        //             DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("userid", EbDbTypes.Int32, request.id) };

        //             var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

        //             Dictionary<string, object> returndata = new Dictionary<string, object>();
        //             List<int> usergroups = new List<int>();
        //             foreach (EbDataRow dr in dt.Tables[0].Rows)
        //             {
        //                 returndata[dr[0].ToString()] = dr[1].ToString();
        //             }

        //             if (dt.Tables.Count > 1)
        //             {
        //                 foreach (EbDataRow dr in dt.Tables[1].Rows)
        //                 {
        //                     usergroups.Add(Convert.ToInt32(dr[0]));
        //                 }
        //                 returndata.Add("usergroups", usergroups);
        //             }
        //             resp.Data = returndata;

        //         }
        //         return resp;
        //     }

        //     public GetUserGroupResponse Any(GetUserGroupRequest request)
        //     {
        //         GetUserGroupResponse resp = new GetUserGroupResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = "";
        //             if (request.id > 0)
        //             {
        //                 sql = @"SELECT id,name,description FROM eb_usergroup WHERE id = @id;
        //                        SELECT id,firstname FROM eb_users WHERE id IN(SELECT userid FROM eb_user2usergroup WHERE groupid = @id AND eb_del = 'F')";


        //                 DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.id) };

        //                 var ds = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);
        //                 Dictionary<string, object> result = new Dictionary<string, object>();
        //                 foreach (var dr in ds.Tables[0].Rows)
        //                 {

        //                     result.Add("name", dr[1].ToString());
        //                     result.Add("description", dr[2].ToString());
        //                 }
        //                 List<int> users = new List<int>();
        //                 if (ds.Tables.Count > 1)
        //                 {
        //                     foreach (EbDataRow dr in ds.Tables[1].Rows)
        //                     {
        //                         users.Add(Convert.ToInt32(dr[0]));
        //                         result.Add(dr[0].ToString(), dr[1]);
        //                     }
        //                     result.Add("userslist", users);
        //                 }
        //                 resp.Data = result;
        //             }
        //             else
        //             {
        //                 sql = "SELECT id,name FROM eb_usergroup";
        //                 var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql);

        //                 Dictionary<string, object> returndata = new Dictionary<string, object>();
        //                 foreach (EbDataRow dr in dt.Tables[0].Rows)
        //                 {
        //                     returndata[dr[0].ToString()] = dr[1].ToString();
        //                 }
        //                 resp.Data = returndata;
        //             }

        //         }
        //         return resp;
        //     }

        //     public GetApplicationObjectsResponse Any(GetApplicationObjectsRequest request)
        //     {
        //         GetApplicationObjectsResponse resp = new GetApplicationObjectsResponse();
        //         using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
        //         {
        //             con.Open();
        //             string sql = @" SELECT 
        //                                 EO.id,EO.obj_name
        //                             FROM 
        //                                 eb_objects EO, eb_objects_ver EOV, eb_objects_status EOS 
        //                             WHERE
        //                                 EO.id = EOV.eb_objects_id AND EOV.id = EOS.eb_obj_ver_id AND EOS.status = 3 
        //                             AND 
        //                                 EOS.id = (SELECT EOS.id FROM eb_objects_status EOS, eb_objects_ver EOV
        //                             WHERE 
        //                                 EOS.eb_obj_ver_id = EOV.id AND EO.id = EOV.eb_objects_id ORDER BY EOS.id DESC LIMIT 1) 
        //                             AND EO.applicationid = @applicationid AND EO.obj_type = @obj_type";
        //             DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("applicationid", EbDbTypes.Int32, request.Id),
        //                 this.EbConnectionFactory.ObjectsDB.GetNewParameter("obj_type", EbDbTypes.Int32, request.objtype) };

        //             var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

        //             Dictionary<string, object> returndata = new Dictionary<string, object>();
        //             foreach (EbDataRow dr in dt.Tables[0].Rows)
        //             {
        //                 returndata[dr[0].ToString()] = dr[1].ToString();
        //             }
        //             resp.Data = returndata;
        //         }
        //         return resp;
        //     }

        [Authenticate]
        public CreateLocationTypeResponse post(CreateLocationTypeRequest request)
        {
            CreateLocationTypeResponse resp = new CreateLocationTypeResponse();
            try
            {
                string query;
                if (request.LocationType.Id > 0)
                    query = $"UPDATE eb_location_types SET type = @type , eb_lastmodified_by = @by, eb_lastmodified_at = {EbConnectionFactory.DataDB.EB_CURRENT_TIMESTAMP} WHERE id = @id  RETURNING id;";
                else
                    query = $"INSERT INTO eb_location_types(type, eb_created_by, eb_created_at) VALUES(@type, @by, {EbConnectionFactory.DataDB.EB_CURRENT_TIMESTAMP}) RETURNING id";
                DbParameter[] parameters = { this.EbConnectionFactory.DataDB.GetNewParameter("type", EbDbTypes.String, request.LocationType.Type),
                    this.EbConnectionFactory.DataDB.GetNewParameter("by", EbDbTypes.Int32, request.UserId),
                    this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.LocationType.Id)
                };

                resp.Id = this.EbConnectionFactory.DataDB.ExecuteScalar<Int32>(query, parameters);
                resp.Status = true;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
            }
            return resp;
        }

        [Authenticate]
        public DeleteLocationTypeResponse Post(DeleteLocationTypeRequest request)
        {
            string query = "UPDATE eb_location_types SET eb_del = 'T' WHERE id = @id ;";
            DbParameter[] parameters = new DbParameter[]{
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.Id) };
            int c = this.EbConnectionFactory.ObjectsDB.DoNonQuery(query, parameters);
            return new DeleteLocationTypeResponse { Id = (c == 1) ? request.Id : 0, Status = (c == 1) ? true : false };
        }

        public EbLocation CreateLocationObject(EbDataRow r)
        {
            return new EbLocation
            {
                LocId = Convert.ToInt32(r[0]),
                ShortName = r[1].ToString(),
                LongName = r[2].ToString(),
                Logo = r[3].ToString(),
                Meta = JsonConvert.DeserializeObject<Dictionary<string, string>>(r[4].ToString()),
                WeekHoliday1 = r[5].ToString(),
                WeekHoliday2 = r[6].ToString(),
                IsGroup = (r[7].ToString() == "T") ? true : false,
                ParentId = Convert.ToInt32(r[8]),
                TypeId = Convert.ToInt32(r[9]),
                TypeName = r[10].ToString(),
                Children = new Dictionary<int, EbLocation>()
            };
        }

        public void RecursivelyGetChildren(Dictionary<int, EbLocation> LocationTree, Node<EbDataRow> node)
        {
            foreach (Node<EbDataRow> Nodedr in node.Children)
            {
                RecursivelyfindKey(LocationTree, Nodedr);
                if (Nodedr.Children.Count > 0)
                    RecursivelyGetChildren(LocationTree, Nodedr);
            }
        }

        public void RecursivelyfindKey(Dictionary<int, EbLocation> Items, Node<EbDataRow> Nodedr)
        {
            int targetkey = Convert.ToInt32(Nodedr.Item["parent_id"]);
            foreach (var item in Items)
            {
                if (item.Key == targetkey)
                {
                    Items[item.Key].Children.Add(Convert.ToInt32(Nodedr.Item["id"]), CreateLocationObject(Nodedr.Item));
                }
                else
                {
                    RecursivelyfindKey(item.Value.Children, Nodedr);
                }
            }
        }
    }
    public class EbSolutionUsers
    {
        public int UserCount { get; set; }

        public Dictionary<int, string> UserList { get; set; }

        public int PlanUserCount { get; set; }
    }
}
