using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
	[Authenticate]
	public class LeadManagementService : EbBaseService
	{
		public LeadManagementService(IEbConnectionFactory _dbf) : base(_dbf) { }

		public GetManageLeadResponse Any(GetManageLeadRequest request)
		{
			string SqlQry = @"SELECT id, longname FROM eb_locations WHERE id > 1;
							  SELECT id, name FROM doctors ORDER BY name;
							  SELECT id, INITCAP(TRIM(fullname)) FROM eb_users WHERE id > 1 ORDER BY fullname;
							SELECT DISTINCT INITCAP(TRIM(clcity)) AS clcity FROM customers WHERE LENGTH(clcity) > 2 ORDER BY clcity;
							SELECT DISTINCT INITCAP(TRIM(clcountry)) AS clcountry FROM customers WHERE LENGTH(clcountry) > 2 ORDER BY clcountry;
							SELECT DISTINCT INITCAP(TRIM(city)) AS city FROM customers WHERE LENGTH(city) > 2 ORDER BY city;
							SELECT district FROM lead_district ORDER BY district;
							SELECT source FROM lead_source ORDER BY source;
							SELECT DISTINCT INITCAP(TRIM(subcategory)) AS subcategory FROM customers WHERE LENGTH(subcategory) > 2 ORDER BY subcategory;
							SELECT status FROM lead_status ORDER BY status;
							SELECT service FROM lead_service ORDER BY service;
							SELECT id, name FROM nurses ORDER BY name;";
			List<DbParameter> paramList = new List<DbParameter>();
			Dictionary<int, string> CostCenter = new Dictionary<int, string>();
			Dictionary<string, int> DocDict = new Dictionary<string, int>();
			Dictionary<string, int> StaffDict = new Dictionary<string, int>();
			Dictionary<string, int> NurseDict = new Dictionary<string, int>();
			Dictionary<string, string> CustomerData = new Dictionary<string, string>();
			List<string> clcityList = new List<string>();
			List<string> clcountryList = new List<string>();
			List<string> cityList = new List<string>();
			List<string> districtList = new List<string>();
			List<string> sourcecategoryList = new List<string>();
			List<string> subcategoryList = new List<string>();
			List<string> statusList = new List<string>();
			List<string> serviceList = new List<string>();
			List<FeedbackEntry> Flist = new List<FeedbackEntry>();
			List<BillingEntry> Blist = new List<BillingEntry>();
			List<SurgeryEntry> Slist = new List<SurgeryEntry>();
			List<string> ImgIds = new List<string>();
			
			int Mode = 0;
			if (request.RequestMode == 1)//edit mode 
			{
				SqlQry += @"SELECT id, eb_loc_id, trdate, genurl, name, dob, genphoffice, profession, genemail, customertype, clcity, clcountry, city,
								typeofcustomer, sourcecategory, subcategory, consultation, picsrcvd, dprefid, sex, district, leadowner
								FROM customers WHERE id = :accountid;
							SELECT id,trdate,status,followupdate,narration, eb_createdby, eb_createddt FROM leaddetails
								WHERE customers_id=:accountid ORDER BY eb_createddt DESC;
							SELECT id,trdate,totalamount,advanceamount,balanceamount,cashreceived,paymentmode,bank,createddt,narration,createdby 
								FROM leadpaymentdetails WHERE customers_id=:accountid ORDER BY balanceamount;
							SELECT id,dateofsurgery,eb_loc_id,createdby,createddt, extractiondone_by,
									implantation_by,consent_by,anaesthesia_by,post_briefing_by,nurses_id
								FROM leadsurgerystaffdetails WHERE customers_id=:accountid ORDER BY createddt DESC;
							SELECT noofgrafts,totalrate,prpsessions,consulted,consultingfeepaid,consultingdoctor,eb_closing,LOWER(TRIM(nature)),consdate,probmonth
								FROM leadratedetails WHERE customers_id=:accountid;

                            SELECT eb_files_ref_id
                                FROM customer_files WHERE customer_id = :accountid;";
//SELECT 
//	B.id, B.filename, B.tags 
//FROM
//	customer_files A,
//	eb_files_ref B
//WHERE
//	A.eb_files_ref_id = B.id AND
//	A.customer_id = :accountid;
				paramList.Add(this.EbConnectionFactory.DataDB.GetNewParameter("accountid", EbDbTypes.Int32, request.AccId));
			}			
			var ds = this.EbConnectionFactory.DataDB.DoQueries(SqlQry, paramList.ToArray());	
			
			foreach (var dr in ds.Tables[0].Rows)
				CostCenter.Add(Convert.ToInt32(dr[0]), dr[1].ToString());
			foreach (var dr in ds.Tables[1].Rows)
				if(!DocDict.ContainsKey(dr[1].ToString()))
					DocDict.Add(dr[1].ToString(), Convert.ToInt32(dr[0]));
			foreach (var dr in ds.Tables[2].Rows)
				if(!StaffDict.ContainsKey(dr[1].ToString()))
					StaffDict.Add(dr[1].ToString(), Convert.ToInt32(dr[0]));
			foreach (var dr in ds.Tables[11].Rows)
				if (!NurseDict.ContainsKey(dr[1].ToString()))
					NurseDict.Add(dr[1].ToString(), Convert.ToInt32(dr[0]));

			foreach (var dr in ds.Tables[3].Rows)
				clcityList.Add(dr[0].ToString());
			foreach (var dr in ds.Tables[4].Rows)
				clcountryList.Add(dr[0].ToString());
			foreach (var dr in ds.Tables[5].Rows)
				cityList.Add(dr[0].ToString());
			foreach (var dr in ds.Tables[6].Rows)
				districtList.Add(dr[0].ToString());

			foreach (var dr in ds.Tables[7].Rows)
				sourcecategoryList.Add(dr[0].ToString());
			foreach (var dr in ds.Tables[8].Rows)
				subcategoryList.Add(dr[0].ToString());
			foreach (var dr in ds.Tables[9].Rows)
				statusList.Add(dr[0].ToString());
			foreach (var dr in ds.Tables[10].Rows)
				serviceList.Add(dr[0].ToString());

			int Qcnt = 12;//Query count first part
			if (ds.Tables.Count > Qcnt && ds.Tables[Qcnt].Rows.Count > 0)
			{
				Mode = 1;
				var dr = ds.Tables[Qcnt].Rows[0];
				CustomerData.Add("accountid", dr[0].ToString());
				CustomerData.Add("eb_loc_id", dr[1].ToString());
				CustomerData.Add("trdate", getStringValue(dr[2]));
				CustomerData.Add("genurl", dr[3].ToString());
				CustomerData.Add("name", dr[4].ToString());
				CustomerData.Add("dob", getStringValue(dr[5]));
				//CustomerData.Add("age", dr[6].ToString());
				CustomerData.Add("genphoffice", dr[6].ToString());
				CustomerData.Add("profession", dr[7].ToString());
				CustomerData.Add("genemail", dr[8].ToString());
				CustomerData.Add("customertype", dr[9].ToString());
				CustomerData.Add("clcity", dr[10].ToString());
				CustomerData.Add("clcountry", dr[11].ToString());
				CustomerData.Add("city", dr[12].ToString());
				CustomerData.Add("typeofcustomer", dr[13].ToString());
				CustomerData.Add("sourcecategory", dr[14].ToString());
				CustomerData.Add("subcategory", dr[15].ToString());
				CustomerData.Add("consultation", dr[16].ToString().ToLower());
				CustomerData.Add("picsrcvd", dr[17].ToString().ToLower());
				CustomerData.Add("dprefid", dr[18].ToString());
				CustomerData.Add("sex", dr[19].ToString());
				CustomerData.Add("district", dr[20].ToString());
				CustomerData.Add("leadowner", dr[21].ToString());
				
				if (ds.Tables[Qcnt + 4].Rows.Count > 0)
				{
					dr = ds.Tables[Qcnt + 4].Rows[0];
					CustomerData.Add("noofgrafts", dr[0].ToString());
					CustomerData.Add("totalrate", dr[1].ToString());
					CustomerData.Add("prpsessions", dr[2].ToString());
					//CustomerData.Add("consulted", dr[3].ToString().ToLower());
					CustomerData.Add("consultingfeepaid", dr[4].ToString().ToLower());
					CustomerData.Add("consultingdoctor", dr[5].ToString());
					CustomerData.Add("closing", dr[6].ToString());
					CustomerData.Add("nature", dr[7].ToString());
					CustomerData.Add("consdate", getStringValue(dr[8]));
					CustomerData.Add("probmonth", (string.IsNullOrEmpty(getStringValue(dr[9])) ? string.Empty: getStringValue(dr[9]).Substring(3).Replace("-", "/")));
				}

				foreach (var i in ds.Tables[Qcnt + 5].Rows)
					ImgIds.Add(i[0].ToString());

				//followup details
				foreach (var i in ds.Tables[Qcnt + 1].Rows)
				{
					Flist.Add(new FeedbackEntry
					{
						Id = Convert.ToInt32(i[0]),
						Date = getStringValue(i[1]),
						Status = i[2].ToString(),
						Fup_Date = getStringValue(i[3]),						
						Comments = i[4].ToString(),
						Created_By = StaffDict.ContainsValue(Convert.ToInt32(i[5]))? StaffDict.FirstOrDefault(x => x.Value == Convert.ToInt32(i[5])).Key : string.Empty,
						Created_Date = getStringValue(i[6], true, true)
					});
				}

				//Billing details
				foreach (var i in ds.Tables[Qcnt + 2].Rows)
				{
					Blist.Add(new BillingEntry
					{
						Id = Convert.ToInt32(i[0]),
						Date = getStringValue(i[1]),
						Total_Amount = Convert.ToInt32(i[2]),
						Amount_Received = Convert.ToInt32(i[3]),
						Balance_Amount = Convert.ToInt32(i[4]),
						Cash_Paid = Convert.ToInt32(i[5]),
						Payment_Mode = i[6].ToString(),
						Bank = i[7].ToString(),
						Clearence_Date = getStringValue(i[8]),
						Narration = i[9].ToString(),
						Created_By = i[10].ToString()
					});
				}

				//surgery details
				foreach (var i in ds.Tables[Qcnt + 3].Rows)
				{
					Slist.Add(new SurgeryEntry
					{
						Id = Convert.ToInt32(i[0]),
						Date = getStringValue(i[1]),
						Branch = CostCenter[Convert.ToInt32(i[2])],
						Created_By = i[3].ToString(),
						Created_Date = getStringValue(i[4]),
						Extract_By = Convert.ToInt32(i[5]),
						Implant_By = Convert.ToInt32(i[6]),
						Consent_By = Convert.ToInt32(i[7]),
						Anaesthesia_By = Convert.ToInt32(i[8]),
						Post_Brief_By = Convert.ToInt32(i[9]),
						Nurse = Convert.ToInt32(i[10])
					});
				}
			}


			return new GetManageLeadResponse {
				RespMode = Mode,
				CostCenterDict = CostCenter,
				DoctorDict = DocDict,
				StaffDict = StaffDict,
				CustomerDataDict = CustomerData,
				FeedbackList = Flist,
				BillingList = Blist,
				SurgeryList = Slist,
				CrntCityList = clcityList,
				CrntCountryList = clcountryList,
				CityList = cityList,
				DistrictList = districtList,
				SourceCategoryList = sourcecategoryList,
				SubCategoryList = subcategoryList,
				ImageIdList = ImgIds,
				StatusList = statusList,
				ServiceList = serviceList,
				NurseDict = NurseDict
			};
		}

		private string getStringValue(object obj)
		{
			obj = (obj == DBNull.Value) ? DateTime.MinValue : obj;
			return (((DateTime)obj).Date != DateTime.MinValue) ? Convert.ToDateTime(obj).ToString("dd-MM-yyyy") : string.Empty;
		}

		private string getStringValue(object obj, bool includetime, bool tolocal)
		{
			obj = (obj == DBNull.Value) ? DateTime.MinValue : obj;
			string format = "dd-MM-yyyy";
			TimeSpan timeSpan = new TimeSpan(0, 0, 0);
			if(includetime)
				format = "dd-MM-yyyy hh:mm tt";
			if(tolocal)
				timeSpan = new TimeSpan(5, 30, 0);
			
			return (((DateTime)obj).Date != DateTime.MinValue) ? Convert.ToDateTime(obj).Add(timeSpan).ToString(format) : string.Empty;
		}
		
		public SaveCustomerResponse Any(SaveCustomerRequest request)
		{
			List<KeyValueType_Field> Fields = JsonConvert.DeserializeObject<List<KeyValueType_Field>>(request.CustomerData);
			var dict = Fields.ToDictionary(x => x.Key);
			KeyValueType_Field found;
			List<DbParameter> parameters = new List<DbParameter>();
			List<DbParameter> parameters2 = new List<DbParameter>();
			string cols = string.Empty, vals = string.Empty;
			string cols2 = string.Empty, vals2 = string.Empty;
			string upcolsvals = string.Empty;
			string upcolsvals2 = string.Empty;

			if (dict.TryGetValue("eb_loc_id", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
				cols += "eb_loc_id,";
				vals += ":eb_loc_id,";
				upcolsvals += "eb_loc_id=:eb_loc_id,";
			}
			if (dict.TryGetValue("trdate", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(found.Value.ToString(), "dd-MM-yyyy", CultureInfo.InvariantCulture))));
				cols += "trdate,";
				vals += ":trdate,";
				upcolsvals += "trdate=:trdate,";
			}
			if (dict.TryGetValue("genurl", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
				cols += "genurl,";
				vals += ":genurl,";
				upcolsvals += "genurl=:genurl,";
			}
			if (dict.TryGetValue("name", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
				cols += "name,";
				vals += ":name,";
				upcolsvals += "name=:name,";
			}
			if (dict.TryGetValue("dob", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(found.Value.ToString(), "dd-MM-yyyy", CultureInfo.InvariantCulture))));
				cols += "dob,";
				vals += ":dob,";
				upcolsvals += "dob=:dob,";
			}
			//if (dict.TryGetValue("age", out found))
			//	parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
			if (dict.TryGetValue("genphoffice", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
				cols += "genphoffice,";
				vals += ":genphoffice,";
				upcolsvals += "genphoffice=:genphoffice,";
			}
			if (dict.TryGetValue("profession", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
				cols += "profession,";
				vals += ":profession,";
				upcolsvals += "profession=:profession,";
			}
			if (dict.TryGetValue("genemail", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
				cols += "genemail,";
				vals += ":genemail,";
				upcolsvals += "genemail=:genemail,";
			}
			if (dict.TryGetValue("customertype", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
				cols += "customertype,";
				vals += ":customertype,";
				upcolsvals += "customertype=:customertype,";
			}
			if (dict.TryGetValue("clcity", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
				cols += "clcity,";
				vals += ":clcity,";
				upcolsvals += "clcity=:clcity,";
			}
			if (dict.TryGetValue("clcountry", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
				cols += "clcountry,";
				vals += ":clcountry,";
				upcolsvals += "clcountry=:clcountry,";
			}
			if (dict.TryGetValue("city", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
				cols += "city,";
				vals += ":city,";
				upcolsvals += "city=:city,";
			}
			if (dict.TryGetValue("typeofcustomer", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
				cols += "typeofcustomer,";
				vals += ":typeofcustomer,";
				upcolsvals += "typeofcustomer=:typeofcustomer,";
			}
			if (dict.TryGetValue("sourcecategory", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
				cols += "sourcecategory,";
				vals += ":sourcecategory,";
				upcolsvals += "sourcecategory=:sourcecategory,";
			}
			if (dict.TryGetValue("subcategory", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
				cols += "subcategory,";
				vals += ":subcategory,";
				upcolsvals += "subcategory=:subcategory,";
			}
			if (dict.TryGetValue("consultation", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.BooleanOriginal, Convert.ToBoolean(found.Value)));
				cols += "consultation,";
				vals += ":consultation,";
				upcolsvals += "consultation=:consultation,";
			}
			if (dict.TryGetValue("picsrcvd", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.BooleanOriginal, Convert.ToBoolean(found.Value)));
				cols += "picsrcvd,";
				vals += ":picsrcvd,";
				upcolsvals += "picsrcvd=:picsrcvd,";
			}
			if (dict.TryGetValue("dprefid", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
				cols += "dprefid,";
				vals += ":dprefid,";
				upcolsvals += "dprefid=:dprefid,";
			}
			if (dict.TryGetValue("sex", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
				cols += "sex,";
				vals += ":sex,";
				upcolsvals += "sex=:sex,";
			}
			if (dict.TryGetValue("district", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
				cols += "district,";
				vals += ":district,";
				upcolsvals += "district=:district,";
			}
			if (dict.TryGetValue("leadowner", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
				cols += "leadowner,";
				vals += ":leadowner,";
				upcolsvals += "leadowner=:leadowner,";
			}
			//------------------------------------------------------
			if (dict.TryGetValue("consdate", out found))
			{
				parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(found.Value.ToString(), "dd-MM-yyyy", CultureInfo.InvariantCulture))));
				cols2 += "consdate,";
				vals2 += ":consdate,";
				upcolsvals2 += "consdate=:consdate,";
			}
			if (dict.TryGetValue("consultingdoctor", out found))
			{
				parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
				cols2 += "consultingdoctor,";
				vals2 += ":consultingdoctor,";
				upcolsvals2 += "consultingdoctor=:consultingdoctor,";
			}
			if (dict.TryGetValue("noofgrafts", out found))
			{
				parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
				cols2 += "noofgrafts,";
				vals2 += ":noofgrafts,";
				upcolsvals2 += "noofgrafts=:noofgrafts,";
			}
			if (dict.TryGetValue("totalrate", out found))
			{
				parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
				cols2 += "totalrate,";
				vals2 += ":totalrate,";
				upcolsvals2 += "totalrate=:totalrate,";
			}
			if (dict.TryGetValue("prpsessions", out found))
			{
				parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
				cols2 += "prpsessions,";
				vals2 += ":prpsessions,";
				upcolsvals2 += "prpsessions=:prpsessions,";
			}
			if (dict.TryGetValue("consultingfeepaid", out found))
			{
				parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.BooleanOriginal, Convert.ToBoolean(found.Value)));
				cols2 += "consultingfeepaid,";
				vals2 += ":consultingfeepaid,";
				upcolsvals2 += "consultingfeepaid=:consultingfeepaid,";
			}
			if (dict.TryGetValue("closing", out found))
			{
				parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
				cols2 += "eb_closing,";
				vals2 += ":closing,";
				upcolsvals2 += "eb_closing=:closing,";
			}
			if (dict.TryGetValue("nature", out found))
			{
				parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
				cols2 += "nature,";
				vals2 += ":nature,";
				upcolsvals2 += "nature=:nature,";
			}
			if (dict.TryGetValue("probmonth", out found))
			{
				parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(found.Value.ToString(), "MM/yyyy", CultureInfo.InvariantCulture))));
				cols2 += "probmonth,";
				vals2 += ":probmonth,";
				upcolsvals2 += "probmonth=:probmonth,";
			}

			//var CrntDateTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("India Standard Time"));

			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("prehead", EbDbTypes.Int32, 50));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("accountcode", EbDbTypes.String, Fields.Find(i => i.Key == "genurl").Value));

			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_createdby", EbDbTypes.Int32, request.UserId));
			//parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_createdat", EbDbTypes.DateTime, CrntDateTime));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_modifiedby", EbDbTypes.Int32, request.UserId));
			//parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_modifiedat", EbDbTypes.DateTime, CrntDateTime));

			parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby", EbDbTypes.String, request.UserName));
			//parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createddt", EbDbTypes.DateTime, CrntDateTime));
			parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifiedby", EbDbTypes.String, request.UserName));
			//parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifieddt", EbDbTypes.DateTime, CrntDateTime));

			parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_createdby", EbDbTypes.Int32, request.UserId));
			//parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_createdat", EbDbTypes.DateTime, CrntDateTime));
			parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_modifiedby", EbDbTypes.Int32, request.UserId));
			//parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_modifiedat", EbDbTypes.DateTime, CrntDateTime));
			

			int accid = 0;
			int rstatus = 0;
			if (request.RequestMode == 0)//New Customer
			{
				string Qry = @"INSERT INTO customers(" + cols + @"accountcode, prehead, eb_createdby, eb_createdat, eb_modifiedby, eb_modifiedat) 
										VALUES(" + vals+ @":accountcode, :prehead, :eb_createdby, NOW(), :eb_modifiedby, NOW())
										 RETURNING id;";
				EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery(Qry, parameters.ToArray());
				accid = Convert.ToInt32(dt.Rows[0][0]);

				string Qry2 = @"INSERT INTO leadratedetails("+cols2+ @"customers_id, accountcode, createdby, createddt, eb_createdby, eb_createdat, eb_modifiedby, eb_modifiedat)
										VALUES (" + vals2+ @":accountid, :accountcode, :createdby, NOW(), :eb_createdby, NOW(), :eb_modifiedby, NOW());";
				parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("accountid", EbDbTypes.Int32, accid));
				parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("accountcode", EbDbTypes.String, Fields.Find(i => i.Key == "genurl").Value));
				rstatus = this.EbConnectionFactory.ObjectsDB.InsertTable(Qry2, parameters2.ToArray());
			}			
			else if (request.RequestMode == 1)
			{
				List<DbParameter> tempParam = new List<DbParameter>();
				if (dict.TryGetValue("accountid", out found))
				{
					parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
					parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
					tempParam.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
					accid = Convert.ToInt32(found.Value);
				}
				parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("prehead", EbDbTypes.Int32, 50));

				string Qry = @"UPDATE customers SET "+ upcolsvals + " eb_modifiedby=:eb_modifiedby, eb_modifiedat=NOW() WHERE prehead = :prehead AND id = :accountid;";
				rstatus = this.EbConnectionFactory.ObjectsDB.UpdateTable(Qry, parameters.ToArray());

				if(rstatus > 0)
				{
					EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery("SELECT id FROM leadratedetails WHERE customers_id = :accountid;", tempParam.ToArray());
					if(dt.Rows.Count > 0)
					{
						string Qry2 = @"UPDATE leadratedetails SET " + upcolsvals2 + "modifiedby=:modifiedby, modifieddt=NOW(), eb_modifiedby=:eb_modifiedby, eb_modifiedat=NOW() WHERE customers_id = :accountid;";
						rstatus += this.EbConnectionFactory.ObjectsDB.UpdateTable(Qry2, parameters2.ToArray()) * 10;
					}
					else
					{
						string Qry2 = @"INSERT INTO leadratedetails(" + cols2 + @"customers_id, accountcode, createdby, createddt, eb_createdby, eb_createdat, eb_modifiedby, eb_modifiedat)
										VALUES (" + vals2 + @":accountid, :accountcode, :createdby, NOW(), :eb_createdby, NOW(), :eb_modifiedby, NOW());";
						parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("accountid", EbDbTypes.Int32, accid));
						parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("accountcode", EbDbTypes.String, Fields.Find(i => i.Key == "genurl").Value));
						rstatus += this.EbConnectionFactory.ObjectsDB.InsertTable(Qry2, parameters2.ToArray()) * 10;
					}
				}				
			}
			List<int> ImgRefId = JsonConvert.DeserializeObject<List<int>>(request.ImgRefId);
			rstatus += Update_Table_Customer_Files(accid, ImgRefId) * 100;

			return new SaveCustomerResponse { Status = (request.RequestMode == 0)? accid :rstatus };
		}

		private int Update_Table_Customer_Files(int accountid, List<int> imagerefid)
		{			
			string query = @"INSERT INTO customer_files(customer_id, eb_files_ref_id) VALUES";
			List<DbParameter> parameters = new List<DbParameter>();
			int i = 0, rstatus = 0;
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("customer_id", EbDbTypes.Int32, accountid));
			for (i = 0; i < imagerefid.Count; i++)
			{
				query += "(:customer_id, :eb_files_ref_id" + i + "),";
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_files_ref_id" + i, EbDbTypes.Int32, imagerefid[i]));
			}
			if (i > 0)
			{
				query = query.Substring(0, query.Length - 1) + ";";
				rstatus = this.EbConnectionFactory.ObjectsDB.InsertTable(query, parameters.ToArray());
			}
			return rstatus;
		}

		public SaveCustomerFollowupResponse Any(SaveCustomerFollowupRequest request)
		{
			int rstatus = 0;
			FeedbackEntry F_Obj = JsonConvert.DeserializeObject<FeedbackEntry>(request.Data);
			List<DbParameter> parameters = new List<DbParameter>();
			//var CrntDateTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("India Standard Time"));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, F_Obj.Id));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("accountid", EbDbTypes.Int32, Convert.ToInt32(F_Obj.Account_Code)));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("trdate", EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(F_Obj.Date, "dd-MM-yyyy", CultureInfo.InvariantCulture))));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("status", EbDbTypes.String, F_Obj.Status));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("followupdate", EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(F_Obj.Fup_Date, "dd-MM-yyyy", CultureInfo.InvariantCulture))));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("narration", EbDbTypes.String, F_Obj.Comments));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby", EbDbTypes.String, request.UserName));
			//parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createddt", EbDbTypes.DateTime, CrntDateTime));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifiedby", EbDbTypes.String, request.UserName));
			//parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifieddt", EbDbTypes.DateTime, CrntDateTime));

			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_createdby", EbDbTypes.Int32, request.UserId));
			//parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_createddt", EbDbTypes.DateTime, CrntDateTime));

			if (true)//update disabled  //if (F_Obj.Id == 0)//new
			{
				string Qry = @"INSERT INTO leaddetails(prehead, customers_id, trdate, status, followupdate, narration, createdby, createddt, eb_createdby, eb_createddt) 
									VALUES('50' , :accountid, :trdate, :status, :followupdate, :narration, :createdby, NOW(), :eb_createdby, NOW());";
				rstatus = this.EbConnectionFactory.ObjectsDB.InsertTable(Qry, parameters.ToArray());
			}
			else//update
			{
				string Qry = @"UPDATE leaddetails 
								SET status=:status, followupdate=:followupdate, narration=:narration, modifiedby = :modifiedby, modifieddt = :modifieddt  
								WHERE prehead = '50' AND customers_id = :accountid AND id=:id;";
				rstatus = this.EbConnectionFactory.ObjectsDB.UpdateTable(Qry, parameters.ToArray());
			}
			return new SaveCustomerFollowupResponse { Status = rstatus };
		}

		public SaveCustomerPaymentResponse Any(SaveCustomerPaymentRequest request)
		{
			int rstatus = 0;
			BillingEntry B_Obj = JsonConvert.DeserializeObject<BillingEntry>(request.Data);
			List<DbParameter> parameters = new List<DbParameter>();
			//var CrntDateTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, TimeZoneInfo.FindSystemTimeZoneById("India Standard Time"));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, B_Obj.Id));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("accountid", EbDbTypes.Int32, B_Obj.Account_Code));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("trdate", EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(B_Obj.Date, "dd-MM-yyyy", CultureInfo.InvariantCulture))));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("totalamount", EbDbTypes.Int32, B_Obj.Total_Amount));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("advanceamount", EbDbTypes.Int32, B_Obj.Amount_Received));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("paymentmode", EbDbTypes.String, B_Obj.Payment_Mode));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("bank", EbDbTypes.String, B_Obj.Bank));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("pdc", EbDbTypes.BooleanOriginal, B_Obj.PDC));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("balanceamount", EbDbTypes.Int32, B_Obj.Balance_Amount));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("cashreceived", EbDbTypes.Int32, B_Obj.Cash_Paid));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby", EbDbTypes.String, request.UserName));
			//parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createddt", EbDbTypes.DateTime, CrntDateTime));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("narration", EbDbTypes.String, B_Obj.Narration));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifiedby", EbDbTypes.String, request.UserName));
			//parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifieddt", EbDbTypes.DateTime, CrntDateTime));

			//parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_createdby", EbDbTypes.Int32, request.UserId));
			//parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_createddt", EbDbTypes.DateTime, CrntDateTime));
			//, eb_createdby, eb_createddt
			//, :eb_createdby, :eb_createddt

			if (true)//update disabled  //if (B_Obj.Id == 0)//new
			{
				string Qry = @"INSERT INTO leadpaymentdetails(prehead,customers_id,trdate,totalamount,advanceamount,paymentmode,bank,balanceamount,cashreceived,createdby,createddt,narration,pdc) 
									VALUES (50,:accountid,:trdate,:totalamount,:advanceamount,:paymentmode,:bank,:balanceamount,:cashreceived,:createdby,NOW(),:narration,:pdc);";
				rstatus = this.EbConnectionFactory.ObjectsDB.InsertTable(Qry, parameters.ToArray());
			}
			else//update
			{
				string Qry = @"UPDATE leadpaymentdetails 
								SET paymentmode = :paymentmode, bank = :bank, cashreceived = :cashreceived,
									narration = :narration, modifiedby = :modifiedby, modifieddt = :modifieddt 
								WHERE customers_id=:accountid AND id = :id;";
				rstatus = this.EbConnectionFactory.ObjectsDB.UpdateTable(Qry, parameters.ToArray());
			}
			return new SaveCustomerPaymentResponse { Status = rstatus };
		}

		public SaveSurgeryDetailsResponse Any(SaveSurgeryDetailsRequest request)
		{
			int rstatus = 0;
			SurgeryEntry S_Obj = JsonConvert.DeserializeObject<SurgeryEntry>(request.Data);
			DbParameter[] parameters1 = {
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("customers_id", EbDbTypes.Int32, S_Obj.Account_Code),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("dateofsurgery", EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(S_Obj.Date, "dd-MM-yyyy", CultureInfo.InvariantCulture))),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_loc_id", EbDbTypes.Int32, S_Obj.Branch),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby", EbDbTypes.String, request.UserName)
			};

			List<DbParameter> parameters = new List<DbParameter>();
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("customers_id", EbDbTypes.Int32, S_Obj.Account_Code));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("dateofsurgery", EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(S_Obj.Date, "dd-MM-yyyy", CultureInfo.InvariantCulture))));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_loc_id", EbDbTypes.Int32, S_Obj.Branch));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby", EbDbTypes.String, request.UserName));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("extractiondone_by", EbDbTypes.Int32, S_Obj.Extract_By));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("implantation_by", EbDbTypes.Int32, S_Obj.Implant_By));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("consent_by", EbDbTypes.Int32, S_Obj.Consent_By));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("anaesthesia_by", EbDbTypes.Int32, S_Obj.Anaesthesia_By));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("post_briefing_by", EbDbTypes.Int32, S_Obj.Post_Brief_By));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("nurses", EbDbTypes.Int32, S_Obj.Nurse));

			
			if (true)//if (S_Obj.Id == 0)//new
			{
				string Qry1 = @"INSERT INTO 
									leadsurgerydetails(customers_id, dateofsurgery, eb_loc_id, createddt, createdby)
								VALUES
									(:customers_id, :dateofsurgery, :eb_loc_id, NOW(), :createdby);";
				this.EbConnectionFactory.ObjectsDB.InsertTable(Qry1, parameters1);
				string Qry = @"INSERT INTO
 									leadsurgerystaffdetails(customers_id, dateofsurgery, eb_loc_id, createddt, createdby, extractiondone_by,
									implantation_by, consent_by, anaesthesia_by, post_briefing_by, nurses_id)
								VALUES
									(:customers_id, :dateofsurgery, :eb_loc_id, NOW(), :createdby, :extractiondone_by,
									:implantation_by, :consent_by, :anaesthesia_by, :post_briefing_by, :nurses);";
				rstatus = this.EbConnectionFactory.ObjectsDB.InsertTable(Qry, parameters.ToArray());
			}
			else//update
			{
				string Qry = @"UPDATE leadsurgerydetails 
								SET dateofsurgery = :dateofsurgery,branch = :branch,patientinstructions = :patientinstructions,doctorinstructions = :doctorinstructions,
									createdby = :createdby,createddt = :createddt, modifiedby = :modifiedby, modifieddt = :modifieddt 
								WHERE accountcode=:accountcode AND id = :id;";
				rstatus = this.EbConnectionFactory.ObjectsDB.UpdateTable(Qry, parameters.ToArray());
			}
			return new SaveSurgeryDetailsResponse { Status = rstatus };
		}

		public LmUniqueCheckResponse Any(LmUniqueCheckRequest request)
		{
			bool rstatus = false;
			DbParameter[] parameters = new DbParameter[] 
			{
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("value", EbDbTypes.String, request.Value.Trim())
			};
			EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery("SELECT id FROM customers WHERE genurl = :value OR genphoffice = :value;", parameters);
			if (dt.Rows.Count == 0)
				rstatus = true;
			return new LmUniqueCheckResponse { Status = rstatus };
		}




		//private int InsertToTable(string TblName, List<KeyValueType_Field> Fields)
		//{
		//	List<DbParameter> parameters = new List<DbParameter>();
		//	string cols = string.Empty;
		//	string vals = string.Empty;
		//	foreach (KeyValueType_Field item in Fields)
		//	{
		//		parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(item.Key, item.Type, item.Value));
		//		cols += item.Key + ",";
		//		vals += ":" + item.Key + ",";
		//	}
		//	string strQry = @"INSERT INTO @tblname@(@cols@) VALUES(@vals@);"
		//						.Replace("@tblname@", TblName)
		//						.Replace("@cols@", cols.Substring(0, cols.Length - 1))
		//						.Replace("@vals@", vals.Substring(0, vals.Length - 1));
		//	this.EbConnectionFactory.ObjectsDB.InsertTable(strQry, parameters.ToArray());
		//	return 1;
		//}

		//private int UpdateToTable(string TblName, List<KeyValueType_Field> Fields, List<KeyValueType_Field> WhereFields)
		//{
		//	List<DbParameter> parameters = new List<DbParameter>();
		//	string s_set = string.Empty;
		//	string s_where = string.Empty;
		//	foreach (KeyValueType_Field item in Fields)
		//	{
		//		parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(item.Key, item.Type, item.Value));
		//		s_set += item.Key + "=:" + item.Key + ",";
		//	}
		//	foreach (KeyValueType_Field item in WhereFields)
		//	{
		//		parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(item.Key, item.Type, item.Value));
		//		s_where += item.Key + "=:" + item.Key + " AND ";
		//	}
		//	string strQry = @"UPDATE @tblname@ SET @str_set@ WHERE @str_where@;"
		//						.Replace("@tblname@", TblName)
		//						.Replace("@str_set@", s_set.Substring(0, s_set.Length - 1))
		//						.Replace("@str_where@", s_where.Substring(0, s_where.Length - 4));
		//	this.EbConnectionFactory.ObjectsDB.UpdateTable(strQry, parameters.ToArray());
		//	return 1;
		//}
	}
}
