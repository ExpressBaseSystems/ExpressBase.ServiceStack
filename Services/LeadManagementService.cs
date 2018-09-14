using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
	public class LeadManagementService : EbBaseService
	{
		public LeadManagementService(IEbConnectionFactory _dbf) : base(_dbf) { }

		public GetManageLeadResponse Any(GetManageLeadRequest request)
		{
			string SqlQry = @"SELECT firmcode, fname FROM firmmaster;
							  SELECT id, name FROM doctors ORDER BY name;
							  SELECT id, name FROM employees ORDER BY name;
							SELECT DISTINCT INITCAP(TRIM(clcity)) AS clcity FROM customers WHERE LENGTH(clcity) > 2 ORDER BY clcity;
							SELECT DISTINCT INITCAP(TRIM(clcountry)) AS clcountry FROM customers WHERE LENGTH(clcountry) > 2 ORDER BY clcountry;
							SELECT DISTINCT INITCAP(TRIM(city)) AS city FROM customers WHERE LENGTH(city) > 2 ORDER BY city;
							SELECT DISTINCT INITCAP(TRIM(sourcecategory)) AS sourcecategory FROM customers WHERE LENGTH(sourcecategory) > 2 ORDER BY sourcecategory;
							SELECT DISTINCT INITCAP(TRIM(subcategory)) AS subcategory FROM customers WHERE LENGTH(subcategory) > 2 ORDER BY subcategory;";
			List<DbParameter> paramList = new List<DbParameter>();
			Dictionary<int, string> CostCenter = new Dictionary<int, string>();
			Dictionary<string, int> DicDict = new Dictionary<string, int>();
			Dictionary<string, int> StaffDict = new Dictionary<string, int>();
			Dictionary<string, string> CustomerData = new Dictionary<string, string>();
			List<string> clcityList = new List<string>();
			List<string> clcountryList = new List<string>();
			List<string> cityList = new List<string>();
			List<string> sourcecategoryList = new List<string>();
			List<string> subcategoryList = new List<string>();
			List<FeedbackEntry> Flist = new List<FeedbackEntry>();
			List<BillingEntry> Blist = new List<BillingEntry>();
			List<SurgeryEntry> Slist = new List<SurgeryEntry>();
			List<string> ImgIds = new List<string>();
			int Mode = 0;
			if (request.RequestMode == 1)//edit mode 
			{
				SqlQry += @"SELECT id, firmcode, trdate, genurl, name, dob, genphoffice, profession, genemail,
								customertype, clcity, clcountry, city, typeofcustomer, sourcecategory, subcategory, consultation, picsrcvd
								FROM customers WHERE id = :accountid;
							SELECT id,trdate,status,followupdate,narration, createdby FROM leaddetails
								WHERE customers_id=:accountid ORDER BY trdate DESC, id;
							SELECT id,trdate,totalamount,advanceamount,balanceamount,cashreceived,paymentmode,bank,createddt,narration,createdby 
								FROM leadpaymentdetails WHERE customers_id=:accountid ORDER BY trdate DESC, balanceamount;
							SELECT id,dateofsurgery,branch,patientinstructions,doctorsinstructions,createdby,createddt 
								FROM leadsurgerydetails WHERE customers_id=:accountid ORDER BY createddt;
							SELECT noofgrafts,totalrate,prpsessions,consulted,consultingfeepaid,consultingdoctor,closing,LOWER(TRIM(nature)),consdate
								FROM leadratedetails WHERE customers_id=:accountid;

                            SELECT eb_files_ref_id
                                FROM customer_files WHERE customer_id = :accountid;";
				paramList.Add(this.EbConnectionFactory.DataDB.GetNewParameter("accountid", EbDbTypes.Int32, request.AccId));
			}			
			var ds = this.EbConnectionFactory.DataDB.DoQueries(SqlQry, paramList.ToArray());	
			
			foreach (var dr in ds.Tables[0].Rows)
				CostCenter.Add(Convert.ToInt32(dr[0]), dr[1].ToString());
			foreach (var dr in ds.Tables[1].Rows)
				DicDict.Add(dr[1].ToString(), Convert.ToInt32(dr[0]));
			foreach (var dr in ds.Tables[2].Rows)
				StaffDict.Add(dr[1].ToString(), Convert.ToInt32(dr[0]));

			foreach (var dr in ds.Tables[3].Rows)
				clcityList.Add(dr[0].ToString());
			foreach (var dr in ds.Tables[4].Rows)
				clcountryList.Add(dr[0].ToString());
			foreach (var dr in ds.Tables[5].Rows)
				cityList.Add(dr[0].ToString());
			foreach (var dr in ds.Tables[6].Rows)
				sourcecategoryList.Add(dr[0].ToString());
			foreach (var dr in ds.Tables[7].Rows)
				subcategoryList.Add(dr[0].ToString());

			if (ds.Tables.Count > 8 && ds.Tables[8].Rows.Count > 0)
			{
				Mode = 1;
				var dr = ds.Tables[8].Rows[0];
				CustomerData.Add("accountid", dr[0].ToString());
				CustomerData.Add("firmcode", dr[1].ToString());
				CustomerData.Add("trdate", Convert.ToDateTime(dr[2]).ToString("dd-MM-yyyy"));
				CustomerData.Add("genurl", dr[3].ToString());
				CustomerData.Add("name", dr[4].ToString());
				CustomerData.Add("dob", Convert.ToDateTime(dr[5]).ToString("dd-MM-yyyy"));
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

				if(ds.Tables[12].Rows.Count > 0)
				{
					dr = ds.Tables[12].Rows[0];
					CustomerData.Add("noofgrafts", dr[0].ToString());
					CustomerData.Add("totalrate", dr[1].ToString());
					CustomerData.Add("prpsessions", dr[2].ToString());
					//CustomerData.Add("consulted", dr[3].ToString().ToLower());
					CustomerData.Add("consultingfeepaid", dr[4].ToString().ToLower());
					CustomerData.Add("consultingdoctor", dr[5].ToString());
					CustomerData.Add("closing", dr[6].ToString());
					CustomerData.Add("nature", dr[7].ToString());
					CustomerData.Add("consdate", Convert.ToDateTime(dr[8]).ToString("dd-MM-yyyy"));
				}

				foreach (var i in ds.Tables[13].Rows)
					ImgIds.Add(i[0].ToString());

				//followup details
				foreach (var i in ds.Tables[9].Rows)
				{
					Flist.Add(new FeedbackEntry
					{
						Id = Convert.ToInt32(i[0]),
						Date = Convert.ToDateTime(i[1]).ToString("dd-MM-yyyy"),
						Status = i[2].ToString(),
						Followup_Date = Convert.ToDateTime(i[3]).ToString("dd-MM-yyyy"),						
						Comments = i[4].ToString(),
						Created_By = i[5].ToString()
					});
				}

				//Billing details
				foreach (var i in ds.Tables[10].Rows)
				{
					Blist.Add(new BillingEntry
					{
						Id = Convert.ToInt32(i[0]),
						Date = Convert.ToDateTime(i[1]).ToString("dd-MM-yyyy"),
						Total_Amount = Convert.ToInt32(i[2]),
						Amount_Received = Convert.ToInt32(i[3]),
						Balance_Amount = Convert.ToInt32(i[4]),
						Cash_Paid = Convert.ToInt32(i[5]),
						Payment_Mode = i[6].ToString(),
						Bank = i[7].ToString(),
						Clearence_Date = Convert.ToDateTime(i[8]).ToString("dd-MM-yyyy"),
						Narration = i[9].ToString(),
						Created_By = i[10].ToString()
					});
				}

				//surgery details
				foreach (var i in ds.Tables[11].Rows)
				{
					Slist.Add(new SurgeryEntry
					{
						Id = Convert.ToInt32(i[0]),
						Date = Convert.ToDateTime(i[1]).ToString("dd-MM-yyyy"),
						Branch = i[2].ToString(),
						Created_By = i[5].ToString(),
						Created_Date = i[6].ToString()
					});
				}
			}


			return new GetManageLeadResponse {
				RespMode = Mode,
				CostCenterDict = CostCenter,
				DoctorDict = DicDict,
				StaffDict = StaffDict,
				CustomerDataDict = CustomerData,
				FeedbackList = Flist,
				BillingList = Blist,
				SurgeryList = Slist,
				CrntCityList = clcityList,
				CrntCountryList = clcountryList,
				CityList = cityList,
				SourceCategoryList = sourcecategoryList,
				SubCategoryList = subcategoryList,
				ImageIdList = ImgIds				
			};
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

			if (dict.TryGetValue("firmcode", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
				cols += "firmcode,";
				vals += ":firmcode,";
			}
			if (dict.TryGetValue("trdate", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(found.Value.ToString(), "dd-MM-yyyy", CultureInfo.InvariantCulture))));
				cols += "trdate,";
				vals += ":trdate,";
			}
			if (dict.TryGetValue("genurl", out found))
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
				cols += "genurl,";
				vals += ":genurl,";
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
				cols2 += "closing,";
				vals2 += ":closing,";
				upcolsvals2 += "closing=:closing,";
			}
			if (dict.TryGetValue("nature", out found))
			{
				parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
				cols2 += "nature,";
				vals2 += ":nature,";
				upcolsvals2 += "nature=:nature,";
			}

			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("prehead", EbDbTypes.Int32, 50));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("accountcode", EbDbTypes.String, Fields.Find(i => i.Key == "genurl").Value));

			int accid = 0;
			int rstatus = 0;
			if (request.RequestMode == 0)//New Customer
			{
				string Qry = @"INSERT INTO customers("+cols+@"accountcode, prehead) 
										VALUES("+vals+@":accountcode, :prehead)
										 RETURNING id;";
				EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery(Qry, parameters.ToArray());
				accid = Convert.ToInt32(dt.Rows[0][0]);

				string Qry2 = @"INSERT INTO leadratedetails("+cols2+ @"customers_id, accountcode)
										VALUES (" + vals2+@":accountid, :accountcode);";
				parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("accountid", EbDbTypes.Int32, accid));
				parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("accountcode", EbDbTypes.String, Fields.Find(i => i.Key == "genurl").Value));
				rstatus = this.EbConnectionFactory.ObjectsDB.InsertTable(Qry2, parameters2.ToArray());
			}			
			else if (request.RequestMode == 1)
			{
				if (dict.TryGetValue("accountid", out found))
				{
					parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
					parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
					accid = Convert.ToInt32(found.Value);
				}
				parameters2.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("prehead", EbDbTypes.Int32, 50));

				string Qry = @"UPDATE customers SET "+ upcolsvals.Substring(0, upcolsvals.Length - 1) +" WHERE prehead = :prehead AND id = :accountid;";
				rstatus = this.EbConnectionFactory.ObjectsDB.UpdateTable(Qry, parameters.ToArray());

				string Qry2 = @"UPDATE leadratedetails SET "+ upcolsvals2.Substring(0, upcolsvals2.Length - 1) +" WHERE customers_id = :accountid;";
				rstatus += this.EbConnectionFactory.ObjectsDB.UpdateTable(Qry2, parameters2.ToArray()) * 10;
			}
			List<int> ImgRefId = JsonConvert.DeserializeObject<List<int>>(request.ImgRefId);
			rstatus += Update_Table_Custmer_Files(accid, ImgRefId) * 100;

			return new SaveCustomerResponse { Status = rstatus };
		}

		private int Update_Table_Custmer_Files(int accountid, List<int> imagerefid)
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
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, F_Obj.Id));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("accountid", EbDbTypes.Int32, Convert.ToInt32(F_Obj.Account_Code)));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("trdate", EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(F_Obj.Date, "dd-MM-yyyy", CultureInfo.InvariantCulture))));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("status", EbDbTypes.String, F_Obj.Status));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("followupdate", EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(F_Obj.Followup_Date, "dd-MM-yyyy", CultureInfo.InvariantCulture))));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("narration", EbDbTypes.String, F_Obj.Comments));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby", EbDbTypes.String, request.UserName));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createddt", EbDbTypes.DateTime, DateTime.Now));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifiedby", EbDbTypes.String, request.UserName));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifieddt", EbDbTypes.DateTime, DateTime.Now));

			if (F_Obj.Id == 0)//new
			{
				string Qry = @"INSERT INTO leaddetails(prehead, customers_id, trdate, status, followupdate, narration, createdby, createddt) 
									VALUES('50' , :accountid, :trdate, :status, :followupdate, :narration, :createdby, :createddt);";
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
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, B_Obj.Id));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("accountid", EbDbTypes.Int32, B_Obj.Account_Code));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("trdate", EbDbTypes.Date, Convert.ToDateTime(DateTime.ParseExact(B_Obj.Date, "dd-MM-yyyy", CultureInfo.InvariantCulture))));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("totalamount", EbDbTypes.Int32, B_Obj.Total_Amount));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("advanceamount", EbDbTypes.Int32, B_Obj.Amount_Received));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("paymentmode", EbDbTypes.String, B_Obj.Payment_Mode));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("bank", EbDbTypes.String, B_Obj.Bank));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("balanceamount", EbDbTypes.Int32, B_Obj.Balance_Amount));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("cashreceived", EbDbTypes.Int32, B_Obj.Cash_Paid));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby", EbDbTypes.String, request.UserName));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createddt", EbDbTypes.DateTime, DateTime.Now));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("narration", EbDbTypes.String, B_Obj.Narration));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifiedby", EbDbTypes.String, request.UserName));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifieddt", EbDbTypes.DateTime, DateTime.Now));
			if (B_Obj.Id == 0)//new
			{
				string Qry = @"INSERT INTO leadpaymentdetails(prehead,customers_id,trdate,totalamount,advanceamount,paymentmode,bank,balanceamount,cashreceived,createdby,createddt,narration) 
									VALUES (50,:accountid,:trdate,:totalamount,:advanceamount,:paymentmode,:bank,:balanceamount,:cashreceived,:createdby,:createddt,:narration);";
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
			List<DbParameter> parameters = new List<DbParameter>();
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, S_Obj.Id));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("accountcode", EbDbTypes.String, S_Obj.Account_Code));

			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("dateofsurgery", EbDbTypes.String, S_Obj.Date));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("branch", EbDbTypes.String, S_Obj.Branch));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("patientinstructions", EbDbTypes.String, ""));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("doctorinstructions", EbDbTypes.String, ""));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby", EbDbTypes.String, request.UserName));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createddt", EbDbTypes.String, DateTime.Now.ToString("dd-MM-yyyy")));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifiedby", EbDbTypes.String, request.UserName));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifieddt", EbDbTypes.String, DateTime.Now.ToString("dd-MM-yyyy")));
			if (S_Obj.Id == 0)//new
			{
				string Qry = @"INSERT INTO leadsurgerydetals(prehead,accountcode,dateofsurgery,branch,patientinstructions,doctorinstructions,createdby,createddt)
													VALUES (50,:accountcode,:dateofsurgery,:branch,:patientinstructions,:doctorinstructions,:createdby,:createddt);";
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
