using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
	public class SupportServices : EbBaseService
	{
		public SupportServices(IEbConnectionFactory _dbf) : base(_dbf) { }

		public SaveBugResponse Post(SaveBugRequest sbreq)
		{
			SaveBugResponse sb = new SaveBugResponse();
			try
			{
				string sql = @"INSERT INTO support_ticket(
                                                    eb_created_by,
                                                    user_type,
                                                    title,
                                                    description,
                                                    priority,
                                                    solution_id,
													eb_created_at,
													modified_at,
													eb_del,
													status,
													type_bg_fr,
													fullname,
													email
													)
													VALUES(
                                                    :usrid,
                                                    :usrtyp,
                                                    :title,
                                                    :descr,
                                                    :priority,
                                                    :solid,
													 NOW(),
													 NOW(),
													:fals,
													:sts,
													:typ,
													:fname,
													:email
                                                )RETURNING id,eb_created_at;";

				DbParameter[] parameters = {
					this.InfraConnectionFactory.DataDB.GetNewParameter("usrid", EbDbTypes.Int32, sbreq.UserId),
					this.InfraConnectionFactory.DataDB.GetNewParameter("usrtyp", EbDbTypes.String, sbreq.usertype),
					this.InfraConnectionFactory.DataDB.GetNewParameter("title", EbDbTypes.String, sbreq.title),
					this.InfraConnectionFactory.DataDB.GetNewParameter("descr", EbDbTypes.String, sbreq.description),
					this.InfraConnectionFactory.DataDB.GetNewParameter("priority", EbDbTypes.String, sbreq.priority),
					this.InfraConnectionFactory.DataDB.GetNewParameter("solid", EbDbTypes.String, sbreq.solutionid),
					this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
					this.InfraConnectionFactory.DataDB.GetNewParameter("sts", EbDbTypes.String, sbreq.status),
					this.InfraConnectionFactory.DataDB.GetNewParameter("typ", EbDbTypes.String,sbreq.type_b_f),
					this.InfraConnectionFactory.DataDB.GetNewParameter("fname", EbDbTypes.String,sbreq.fullname),
					this.InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String,sbreq.email)
					};

				EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters);
				sb.Id = Convert.ToInt32(dt.Rows[0][0]);
				string datecrt = dt.Rows[0][1].ToString();

				if (sb.Id > 0)
				{
					string cx = sb.Id.ToString();

					//for making id 6 digit with intial position 0 in case of single digit

					string l = string.Format("select lpad('{0}',10,'0');", cx);
					EbDataTable dt3 = this.InfraConnectionFactory.DataDB.DoQuery(l);
					string sbgf = null;
					if (sbreq.type_b_f.Equals("Bug"))
					{
						sbgf = "IS" + dt3.Rows[0][0];

					}
					else if (sbreq.type_b_f.Equals("FeatureRequest"))
					{
						sbgf = "IS" + dt3.Rows[0][0];
					}
					string k = String.Format("UPDATE support_ticket SET ticket_id = :tkt WHERE id=:tktid and eb_del=:fls;", sb.Id);
					DbParameter[] param = {
						this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.Int32, sb.Id),
						this.InfraConnectionFactory.DataDB.GetNewParameter("fls", EbDbTypes.String, "F"),
						this.InfraConnectionFactory.DataDB.GetNewParameter("tkt", EbDbTypes.String,sbgf)
					};
					int dt2 = this.InfraConnectionFactory.DataDB.DoNonQuery(k, param);

					//for history
					string sql6 = @"INSERT INTO  support_ticket_history(
																	ticket_id,
																	eb_del,
																	field,
																	value,
																	username,
																	field_id,
																	eb_created_at,
																	solution_id

																	)
																	VALUES(
																		:tktid,
																		:fals,
																		:fld,
																		:val,
																		:usrname,
																		:fldid,
																		NOW(),
																		:slid
																		)RETURNING id;";
					DbParameter[] parameters6 = {
								this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, sbgf),
								this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
								this.InfraConnectionFactory.DataDB.GetNewParameter("fld", EbDbTypes.String, SupportTicketFields.date_created.ToString()),
								this.InfraConnectionFactory.DataDB.GetNewParameter("val", EbDbTypes.String,datecrt),
								this.InfraConnectionFactory.DataDB.GetNewParameter("fldid", EbDbTypes.Int32,  SupportTicketFields.date_created),
								this.InfraConnectionFactory.DataDB.GetNewParameter("usrname", EbDbTypes.String,sbreq.fullname),
								this.InfraConnectionFactory.DataDB.GetNewParameter("slid", EbDbTypes.String, sbreq.solutionid),
								};

					EbDataTable dt6 = this.InfraConnectionFactory.DataDB.DoQuery(sql6, parameters6);
					var ide = Convert.ToInt32(dt6.Rows[0][0]);


					//to upload images
					FileUploadCls flupcl = new FileUploadCls();
					if (sbreq.Fileuploadlst.Count > 0)
					{
						for (var i = 0; i < sbreq.Fileuploadlst.Count; i++)
						{
							byte[] sa = sbreq.Fileuploadlst[i].Filecollection;

							string sql3 = @"INSERT INTO  support_ticket_files(
																	ticket_id,
																	eb_del,
																	img_bytea,
																	content_type,
																	file_name,
																	solution_id

																	)
																	VALUES(
																		:tktid,
																		:fals,
																		:filebt,
																		:cnttyp,
																		:flname,
																		:slid
																		)RETURNING id;";
							DbParameter[] parameters3 = {
								this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, sbgf),
								this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
								this.InfraConnectionFactory.DataDB.GetNewParameter("filebt", EbDbTypes.Bytea,sbreq.Fileuploadlst[i].Filecollection),
								this.InfraConnectionFactory.DataDB.GetNewParameter("cnttyp", EbDbTypes.String, sbreq.Fileuploadlst[i].ContentType),
								this.InfraConnectionFactory.DataDB.GetNewParameter("flname", EbDbTypes.String, sbreq.Fileuploadlst[i].FileName),
								this.InfraConnectionFactory.DataDB.GetNewParameter("slid", EbDbTypes.String, sbreq.solutionid),
								};

							EbDataTable dt4 = this.InfraConnectionFactory.DataDB.DoQuery(sql3, parameters3);
							var iden = Convert.ToInt32(dt4.Rows[0][0]);

						}
					}
				}
				else
				{
					sb.ErMsg = "Error occured while saving";
				}
			}
			catch (Exception e)
			{
				Console.WriteLine("Exception: " + e.Message + e.StackTrace);
				sb.ErMsg = "Unexpected error occurred";
			}
			return sb;
		}



		//to fetch solution id,name from tenant table  to show in dropdown

		public TenantSolutionsResponse Post(TenantSolutionsRequest tsreq)
		{
			TenantSolutionsResponse tr = new TenantSolutionsResponse();

			try
			{
				string sql = @"SELECT 
								isolution_id,
								solution_name,
								esolution_id 
								FROM eb_solutions 
							WHERE 
									tenant_id=:tktid
									AND 
									eb_del=false;";
				DbParameter[] parameters3 = {
				this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.Int32,  tsreq.UserId),
				};
				EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters3);


				for (int i = 0; i < dt.Rows.Count; i++)
				{
					tr.solid.Add(dt.Rows[i][0].ToString());
					tr.solname.Add(dt.Rows[i][1].ToString());
					tr.soldispid.Add(dt.Rows[i][2].ToString());
				}
			}
			catch (Exception e)
			{
				Console.WriteLine("Exception: " + e.Message + e.StackTrace);
				tr.ErMsg = "Unexpected error occurred";
			}
			return tr;
		}


		//to fetch all details of tickets of corresponding user of that corresponding solution to show as tables
		public FetchSupportResponse Post(FetchSupportRequest fsreq)
		{
			FetchSupportResponse fr = new FetchSupportResponse();
			try
			{
				DateTime tdate = DateTime.UtcNow;
				if (fsreq.WhichConsole.Equals("tc"))
				{
					string sql2 = @"SELECT 
									title, 
									description,
									priority, 
									solution_id, 
									eb_created_at, 
									status, 
									remarks, 
									assigned_to, 
									type_bg_fr,
									ticket_id
									FROM support_ticket
								WHERE 
									eb_del=:fls 
									AND 
									solution_id 
								IN
								(SELECT 
									isolution_id 
								FROM
									eb_solutions 
								WHERE 
									tenant_id=:tndid 
								AND
									eb_del=false
								
								)
								ORDER BY id ;";
					DbParameter[] parameters2 = {
						this.InfraConnectionFactory.DataDB.GetNewParameter("tndid", EbDbTypes.Int32, fsreq.UserId),
						this.InfraConnectionFactory.DataDB.GetNewParameter("fls", EbDbTypes.String, "F")
					};

					EbDataTable dt2 = this.InfraConnectionFactory.DataDB.DoQuery(sql2, parameters2);

					if (dt2.Rows.Count > 0)
					{
						for (int i = 0; i < dt2.Rows.Count; i++)
						{
							SupportTktCls st = new SupportTktCls();
							st.title = dt2.Rows[i][0].ToString();
							st.description = dt2.Rows[i][1].ToString();
							st.priority = dt2.Rows[i][2].ToString();
							st.solutionid = dt2.Rows[i][3].ToString();
							DateTime stdate = (DateTime)dt2.Rows[i][4];
							st.Age = (((tdate - stdate).Days).ToString() + "D" + " " + ((tdate - stdate).Hours).ToString() + "h");
							st.lstmodified = dt2.Rows[i][4].ToString();
							st.status = dt2.Rows[i][5].ToString();
							st.remarks = dt2.Rows[i][6].ToString();
							st.assignedto = dt2.Rows[i][7].ToString();
							st.type_b_f = dt2.Rows[i][8].ToString();
							st.ticketid = dt2.Rows[i][9].ToString();
							fr.supporttkt.Add(st);
						}
					}
					else
					{
						fr.ErMsg = "No tickets found";
					}
				}
				else
				{
					string sql3 = @"SELECT 
									title, 
									description,
									priority, 
									solution_id, 
									eb_created_at, 
									status, 
									remarks, 
									assigned_to, 
									type_bg_fr,
									ticket_id
									FROM support_ticket
								WHERE 
									solution_id =:sln 
								AND 
									eb_del=:fls
								ORDER BY id
								;";
					DbParameter[] parameters3 = {
					this.InfraConnectionFactory.DataDB.GetNewParameter("sln", EbDbTypes.String, fsreq.SolnId),
					this.InfraConnectionFactory.DataDB.GetNewParameter("fls", EbDbTypes.String, "F")
					};

					EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql3, parameters3);
					if (dt.Rows.Count > 0)
					{
						for (int i = 0; i < dt.Rows.Count; i++)
						{
							SupportTktCls st = new SupportTktCls();
							st.title = dt.Rows[i][0].ToString();
							st.description = dt.Rows[i][1].ToString();
							st.priority = dt.Rows[i][2].ToString();
							st.solutionid = dt.Rows[i][3].ToString();
							DateTime stdate = (DateTime)dt.Rows[i][4];
							st.Age = (((tdate - stdate).Days).ToString() + "D" + " " + ((tdate - stdate).Hours).ToString() + "h");
							st.lstmodified = dt.Rows[i][4].ToString();
							st.status = dt.Rows[i][5].ToString();
							st.remarks = dt.Rows[i][6].ToString();
							st.assignedto = dt.Rows[i][7].ToString();
							st.type_b_f = dt.Rows[i][8].ToString();
							st.ticketid = dt.Rows[i][9].ToString();
							fr.supporttkt.Add(st);
						}
					}
					else
					{
						fr.ErMsg = "NO tickets found";
					}
				}

			}
			catch (Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
				fr.ErMsg = "Unexpected error occurred";
			}
			return fr;
		}

		//to fetch all details of tickets of  user  to show as tables of admin solution
		public AdminSupportResponse Post(AdminSupportRequest asreq)
		{
			AdminSupportResponse asr = new AdminSupportResponse();
			try
			{
				DateTime tdate = DateTime.UtcNow;
				string sql2 = @"SELECT 
									title, 
									description,
									priority, 
									solution_id, 
									eb_created_at, 
									status, 
									remarks, 
									assigned_to, 
									type_bg_fr,
									ticket_id
								FROM 
									support_ticket
								WHERE 
									eb_del='F'
								ORDER BY id
								 
								;";
				EbDataTable dt2 = this.InfraConnectionFactory.DataDB.DoQuery(sql2);
				if (dt2.Rows.Count > 0)
				{
					for (int i = 0; i < dt2.Rows.Count; i++)
					{
						SupportTktCls st = new SupportTktCls();
						st.title = dt2.Rows[i][0].ToString();
						st.description = dt2.Rows[i][1].ToString();
						st.priority = dt2.Rows[i][2].ToString();
						st.solutionid = dt2.Rows[i][3].ToString();
						DateTime stdate = (DateTime)dt2.Rows[i][4];
						st.Age = (((tdate - stdate).Days).ToString() + "D" + " " + ((tdate - stdate).Hours).ToString() + "h");
						st.lstmodified = dt2.Rows[i][4].ToString();
						st.status = dt2.Rows[i][5].ToString();
						st.remarks = dt2.Rows[i][6].ToString();
						st.assignedto = dt2.Rows[i][7].ToString();
						st.type_b_f = dt2.Rows[i][8].ToString();
						st.ticketid = dt2.Rows[i][9].ToString();
						asr.supporttkt.Add(st);
					}
				}
				else
				{
					asr.ErMsg = "NO tickets found";
				}

			}
			catch (Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
				asr.ErMsg = "Unexpected error occurred";
			}
			return asr;
		}

		// fectch complete details of ticket and show it in edit /view ticket
		public SupportDetailsResponse Post(SupportDetailsRequest sdreq)
		{
			SupportDetailsResponse sd = new SupportDetailsResponse();
			string sql = null;
			string sql1 = null;

			var parameters = new List<DbParameter>();
			var parameters1 = new List<DbParameter>();
			try
			{
				if (sdreq.SolnId.Equals("admin"))
				{
					sql = @"SELECT 
								title, 
								description,
								priority, 
								solution_id, 
								modified_at, 
								status, 
								remarks, 
								assigned_to, 
								type_bg_fr,
								eb_created_at
								FROM support_ticket
							WHERE 
								ticket_id =:ticketno 
							AND 
							eb_del=:fals
							;";

					parameters.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("ticketno", EbDbTypes.String, sdreq.ticketno));
					parameters.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"));


					sql1 = @"SELECT
								id,
								img_bytea,
								content_type,
								file_name 
								from 
								support_ticket_files
							where 
								ticket_id =:ticketno 
							AND
								eb_del=:fals
							;";

					parameters1.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("ticketno", EbDbTypes.String, sdreq.ticketno));
					parameters1.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"));

				}
				else
				{
					if (sdreq.Usertype.Equals("tc"))
					{
						sql = @"SELECT 
									title, 
									description,
									priority, 
									solution_id, 
									modified_at, 
									status, 
									remarks, 
									assigned_to, 
									type_bg_fr,
									eb_created_at
									FROM support_ticket
								WHERE 
									ticket_id =:ticketno
								AND 
									eb_del=:fals 
								AND 
									solution_id
								IN
								(SELECT 
										isolution_id 
									FROM
										eb_solutions
									WHERE 
										tenant_id =:UserId
									AND 
										eb_del = false);";


						parameters.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("ticketno", EbDbTypes.String, sdreq.ticketno));
						parameters.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("UserId", EbDbTypes.Int32, sdreq.UserId));
						parameters.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"));

						sql1 = @"SELECT
									id,
									img_bytea,
									content_type,
									file_name 
									from 
									support_ticket_files
								where 
									ticket_id =:ticketno 
								AND
									eb_del=:fals 
								AND 
									solution_id
								IN
								(SELECT
									isolution_id  
								FROM 
									eb_solutions 
								WHERE 
									tenant_id =:UserId 
								AND 
									eb_del = false);";

						parameters1.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("ticketno", EbDbTypes.String, sdreq.ticketno));
						parameters1.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("UserId", EbDbTypes.Int32, sdreq.UserId));
						parameters1.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"));

					}
					else
					{
						sql = string.Format(@"SELECT 
												title, 
												description,
												priority, 
												solution_id, 
												modified_at, 
												status, 
												remarks, 
												assigned_to, 
												type_bg_fr,
												eb_created_at
											FROM
												support_ticket
											WHERE 
												ticket_id =:ticketno 
											AND
												eb_del=:fals 
											AND
												solution_id=:SolnId;");

						parameters.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("ticketno", EbDbTypes.String, sdreq.ticketno));
						parameters.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("SolnId", EbDbTypes.String, sdreq.SolnId));
						parameters.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"));


						sql1 = string.Format(@"SELECT 
													id,
													img_bytea,
													content_type,
													file_name 
													from 
													support_ticket_files
												where
													ticket_id =:ticketno
												AND
													eb_del=:fals 
												AND 
													solution_id=:SolnId;");

						parameters1.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("ticketno", EbDbTypes.String, sdreq.ticketno));
						parameters1.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("SolnId", EbDbTypes.String, sdreq.SolnId));
						parameters1.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"));

					}
				}
				DbParameter[] param = parameters.ToArray();
				DbParameter[] param1 = parameters1.ToArray();

				EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, param);
				SupportTktCls st = new SupportTktCls();
				if (dt.Rows.Count > 0)
				{
					for (int i = 0; i < dt.Rows.Count; i++)
					{

						st.title = dt.Rows[i][0].ToString();
						st.description = dt.Rows[i][1].ToString();
						st.priority = dt.Rows[i][2].ToString();
						st.solutionid = dt.Rows[i][3].ToString();
						st.lstmodified = dt.Rows[i][4].ToString();
						st.status = dt.Rows[i][5].ToString();
						st.remarks = dt.Rows[i][6].ToString();
						st.assignedto = dt.Rows[i][7].ToString();
						st.type_b_f = dt.Rows[i][8].ToString();
						st.createdat = dt.Rows[i][9].ToString();
						st.ticketid = sdreq.ticketno;

					}
				}
				else
				{
					sd.ErMsg = "Error occured while retrieving details";
				}

				EbDataTable dt2 = this.InfraConnectionFactory.DataDB.DoQuery(sql1, param1);

				for (int i = 0; i < dt2.Rows.Count; i++)
				{
					FileUploadCls flupcls = new FileUploadCls();

					flupcls.Filecollection = ((Byte[])(dt2.Rows[i][1]));
					flupcls.FileId = ((int)(dt2.Rows[i][0]));
					flupcls.FileName = dt2.Rows[i][3].ToString();
					flupcls.ContentType = dt2.Rows[i][2].ToString();

					//check for file type

					//convert file to base 64 and to url
					string fileBase64Data = Convert.ToBase64String(flupcls.Filecollection);
					if ((flupcls.ContentType == "image/jpeg") || (flupcls.ContentType == "image/jpg") || (flupcls.ContentType == "image/png"))
					{
						flupcls.FileDataURL = string.Format("data:image/png;base64,{0}", fileBase64Data);
					}
					else if ((flupcls.ContentType == "application/pdf"))
					{
						flupcls.FileDataURL = string.Format("data:application/pdf;base64,{0}", fileBase64Data);
					}


					st.Fileuploadlst.Add(flupcls);
				}
				sd.supporttkt.Add(st);
				sd.SdrStatus = true;
			}
			catch (Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
				sd.ErMsg = "Unexpected error occurred";
			}
			return sd;
		}



		public UpdateTicketResponse Post(UpdateTicketRequest utreq)
		{
			UpdateTicketResponse utr = new UpdateTicketResponse();
			utr.status = false;





			try
			{
				string tem = string.Empty;
				List<string> FieldKey = new List<string>();
				List<string> FieldValue = new List<string>();
				List<DbParameter> p = new List<DbParameter>();

				string[] DBcolms = new string[] { "title", "description", "priority", "solution_id", "type_bg_fr", "assigned_to", "status", "comment", "files", "date_created" };


				for (int i = 0; i < DBcolms.Length; i++)
				{
					if (utreq.chngedtkt.ContainsKey(DBcolms[i]))
					{
						FieldKey.Add(DBcolms[i]);
						FieldValue.Add(":" + DBcolms[i]);
						p.Add(this.InfraConnectionFactory.DataDB.GetNewParameter(":" + DBcolms[i], EbDbTypes.String, utreq.chngedtkt[DBcolms[i]]));

					}
				}
				for (int j = 0; j < FieldKey.Count; j++)
				{
					tem += FieldKey[j] + "=" + FieldValue[j] + ",";
				}

				tem = tem.Remove(tem.Length - 1, 1);
				string k = String.Format(@"UPDATE 
										support_ticket 
										SET
										{0}
										WHERE 
											ticket_id=:tktid
                                            and eb_del=:fals", tem
											);

				p.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, utreq.ticketid));
				p.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"));
				DbParameter[] parameters = p.ToArray();
				int dt = this.InfraConnectionFactory.DataDB.DoNonQuery(k, parameters);

				if (dt == 1)
				{
					if (utreq.chngedtkt.ContainsKey("solution_id"))
					{
						string k8 = String.Format(@"UPDATE 
											support_ticket_files 
											SET
											solution_id=:slutn 
											WHERE 
												ticket_id=:tktid
												and eb_del=:fals"
												);


						DbParameter[] parameters8 = {
									this.InfraConnectionFactory.DataDB.GetNewParameter("slutn", EbDbTypes.String, utreq.chngedtkt["solution_id"]),
									this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
									this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, utreq.ticketid)
									};

						int dt5 = this.InfraConnectionFactory.DataDB.DoNonQuery(k8, parameters8);

					}
				}
					if (dt == 0)
				{
					utr.ErMsg = "Unexpected error occurred while updating";
				}
				

				//to insert into history
				DateTime tdate = DateTime.UtcNow;
				string sql6 = @"INSERT INTO  support_ticket_history(
																ticket_id,
																eb_del,
																field,
																value,
																username,
																field_id,
																eb_created_at,
																solution_id
																)
																VALUES(
																	:tktid,
																	:fals,
																	:fld,
																	:val,
																	:usrname,
																	:fldid,
																	:nwtime,
																	:slid
																	)RETURNING id;";


				List<string> klist = new List<string>(utreq.chngedtkt.Keys);
				List<string> vlist = new List<string>(utreq.chngedtkt.Values);
				for (int j = 0; j < utreq.chngedtkt.Count; j++)
				{
					DbParameter[] parameters6 = {
							this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, utreq.ticketid),
							this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
							this.InfraConnectionFactory.DataDB.GetNewParameter("fld", EbDbTypes.String,klist[j]),
							this.InfraConnectionFactory.DataDB.GetNewParameter("val", EbDbTypes.String,vlist[j] ),
							this.InfraConnectionFactory.DataDB.GetNewParameter("fldid", EbDbTypes.Int32,  (int)SupportTicketFields.status),
							this.InfraConnectionFactory.DataDB.GetNewParameter("usrname", EbDbTypes.String, utreq.usrname),
							this.InfraConnectionFactory.DataDB.GetNewParameter("nwtime", EbDbTypes.DateTime, tdate),
							this.InfraConnectionFactory.DataDB.GetNewParameter("slid", EbDbTypes.String, utreq.solution_id),
							};

					EbDataTable dt6 = this.InfraConnectionFactory.DataDB.DoQuery(sql6, parameters6);
					var ide = Convert.ToInt32(dt6.Rows[0][0]);

				}

				//remove previouse upload files ie set false
				if (dt > 0)
				{


					if (utreq.Filedel.Length > 0)
					{
						for (var m = 0; m < utreq.Filedel.Length; m++)
						{
							string k1 = String.Format(@"UPDATE 
											support_ticket_files 
											SET
											eb_del=:tru 
											WHERE 
												ticket_id=:tktid
												and eb_del=:fals
												and id=:fileid"
												);


							DbParameter[] parameters5 = {
									this.InfraConnectionFactory.DataDB.GetNewParameter("tru", EbDbTypes.String, "T"),
									this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
									this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, utreq.ticketid),
									this.InfraConnectionFactory.DataDB.GetNewParameter("fileid", EbDbTypes.Int32, utreq.Filedel[m])
									};

							int dt5 = this.InfraConnectionFactory.DataDB.DoNonQuery(k1, parameters5);

						}

					}



					//to upload images
					FileUploadCls flupcl = new FileUploadCls();
					if (utreq.Fileuploadlst.Count > 0)
					{
						for (var i = 0; i < utreq.Fileuploadlst.Count; i++)
						{
							byte[] sa = utreq.Fileuploadlst[i].Filecollection;

							string sql3 = @"INSERT INTO  support_ticket_files(
																		ticket_id,
																		eb_del,
																		img_bytea,
																		content_type,
																		file_name,
																		solution_id															
																		)
																		VALUES(
																		:tktid,
																		:fals,
																		:filebt,
																		:cnttyp,
																		:flname,
																		:solu
																		)RETURNING id;";
							DbParameter[] parameters3 = {
									this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
									this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, utreq.ticketid),
									this.InfraConnectionFactory.DataDB.GetNewParameter("solu", EbDbTypes.String, utreq.solution_id),
									this.InfraConnectionFactory.DataDB.GetNewParameter("filebt", EbDbTypes.Bytea,utreq.Fileuploadlst[i].Filecollection),
									this.InfraConnectionFactory.DataDB.GetNewParameter("cnttyp", EbDbTypes.String, utreq.Fileuploadlst[i].ContentType),
									this.InfraConnectionFactory.DataDB.GetNewParameter("flname", EbDbTypes.String, utreq.Fileuploadlst[i].FileName),
									};

							EbDataTable dt4 = this.InfraConnectionFactory.DataDB.DoQuery(sql3, parameters3);
							var iden = Convert.ToInt32(dt4.Rows[0][0]);
							if (dt4.Rows.Count < 0)
							{
								utr.ErMsg = "Unexpected error occurred while updating";
							}

						}
					}




				}
				utr.status = true;
			}
			catch (Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
				utr.ErMsg = "Unexpected error occurred";
			}
			return utr;
		}

		public UpdateTicketAdminResponse Post(UpdateTicketAdminRequest utreq)
		{
			UpdateTicketAdminResponse utr = new UpdateTicketAdminResponse();
			utr.status = false;
			try
			{
				string k = String.Format(@"UPDATE 
										support_ticket 
										SET
										status = :sts,
										assigned_to = :asgned,
										remarks=:rmrk,
										type_bg_fr=:typ
										WHERE 
											ticket_id=:tktid
                                            and eb_del=:fals
											and solution_id = :soluid"
											);
				DbParameter[] parameters = {
					this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, utreq.Ticketid),
					this.InfraConnectionFactory.DataDB.GetNewParameter("sts", EbDbTypes.String, utreq.Status),
					this.InfraConnectionFactory.DataDB.GetNewParameter("asgned", EbDbTypes.String, utreq.AssignTo),
					this.InfraConnectionFactory.DataDB.GetNewParameter("rmrk", EbDbTypes.String, utreq.Remarks),
					this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
					this.InfraConnectionFactory.DataDB.GetNewParameter("soluid", EbDbTypes.String, utreq.Solution_id),
					this.InfraConnectionFactory.DataDB.GetNewParameter("typ", EbDbTypes.String, utreq.Type_f_b)


				};
				int dt = this.InfraConnectionFactory.DataDB.DoNonQuery(k, parameters);
				if (dt == 0)
				{
					utr.ErMsg = "Unexpected error occurred while updating";
				}
				else
				{
					//if (utreq.History_fv.Count > 0)
					//{

					//	string sql6 = @"INSERT INTO  support_ticket_history(
					//												ticket_id,
					//												eb_del,
					//												field,
					//												value,
					//												username,
					//												field_id,
					//												eb_created_at,
					//												solution_id

					//												)
					//												VALUES(
					//													:tktid,
					//													:fals,
					//													:fld,
					//													:val,
					//													:usrname,
					//													:fldid,
					//													NOW(),
					//													:slid
					//													)RETURNING id;";
					//	DbParameter[] parameters6 = {
					//			this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, utreq.ticketid),
					//			this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
					//			this.InfraConnectionFactory.DataDB.GetNewParameter("fld", EbDbTypes.String,""),
					//			this.InfraConnectionFactory.DataDB.GetNewParameter("val", EbDbTypes.String, ""),
					//			this.InfraConnectionFactory.DataDB.GetNewParameter("fldid", EbDbTypes.Int32,  1),
					//			this.InfraConnectionFactory.DataDB.GetNewParameter("usrname", EbDbTypes.String, utreq.usrname),
					//			this.InfraConnectionFactory.DataDB.GetNewParameter("slid", EbDbTypes.String, utreq.solution_id),
					//			};

					//	EbDataTable dt6 = this.InfraConnectionFactory.DataDB.DoQuery(sql6, parameters6);
					//	var ide = Convert.ToInt32(dt6.Rows[0][0]);

					//}
				}

				utr.status = true;
			}
			catch (Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
				utr.ErMsg = "Unexpected error occurred";
			}
			return utr;
		}


		public FetchAdminsResponse Post(FetchAdminsRequest tsreq)
		{
			FetchAdminsResponse far = new FetchAdminsResponse();

			try
			{
				string sql = @"SELECT 
								fullname
								FROM eb_users 
							WHERE
								id > 1 AND
									statusid=0
									AND 
									eb_del='F';";

				EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql);


				for (int i = 0; i < dt.Rows.Count; i++)
				{
					far.AdminNames.Add(dt.Rows[i][0].ToString());
				}
			}
			catch (Exception e)
			{
				Console.WriteLine("Exception: " + e.Message + e.StackTrace);
				far.ErMsg = "Unexpected error occurred";
			}
			return far;
		}


		public ChangeStatusResponse Post(ChangeStatusRequest chstreq)
		{
			ChangeStatusResponse chst = new ChangeStatusResponse();
			try
			{
				DateTime tdate = DateTime.UtcNow;
				string k = String.Format(@"UPDATE 
										support_ticket 
										SET
										status = :sts
										WHERE 
											ticket_id=:tktid
                                            and eb_del=:fals"
											);
				DbParameter[] parameters = {
					this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, chstreq.TicketNo),
					this.InfraConnectionFactory.DataDB.GetNewParameter("sts", EbDbTypes.String,chstreq.NewStatus ),
					this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),

					};
				int dt = this.InfraConnectionFactory.DataDB.DoNonQuery(k, parameters);
				if (dt == 0)
				{
					chst.ErMsg = "Unexpected error occurred while updating";
				}
				else
				{
					string sql6 = @"INSERT INTO  support_ticket_history(
																	ticket_id,
																	eb_del,
																	field,
																	value,
																	username,
																	field_id,
																	eb_created_at,
																	solution_id

																	)
																	VALUES(
																		:tktid,
																		:fals,
																		:fld,
																		:val,
																		:usrname,
																		:fldid,
																		:tdate,
																		:slid
																		)RETURNING id;";
					DbParameter[] parameters6 = {
								this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String,  chstreq.TicketNo),
							this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
							this.InfraConnectionFactory.DataDB.GetNewParameter("fld", EbDbTypes.String,SupportTicketFields.status.ToString()),
							this.InfraConnectionFactory.DataDB.GetNewParameter("val", EbDbTypes.String,chstreq.NewStatus ),
							this.InfraConnectionFactory.DataDB.GetNewParameter("fldid", EbDbTypes.Int32,  (int)SupportTicketFields.status),
							this.InfraConnectionFactory.DataDB.GetNewParameter("usrname", EbDbTypes.String, chstreq.UserName),
							this.InfraConnectionFactory.DataDB.GetNewParameter("tdate", EbDbTypes.DateTime, tdate),
							this.InfraConnectionFactory.DataDB.GetNewParameter("slid", EbDbTypes.String, chstreq.Solution_id),
								};

					EbDataTable dt6 = this.InfraConnectionFactory.DataDB.DoQuery(sql6, parameters6);
					var ide = Convert.ToInt32(dt6.Rows[0][0]);
				}
				chst.RtnStatus = true;

			}
			catch (Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
				chst.ErMsg = "Unexpected error occurred";
			}
			return chst;
		}

		public SupportHistoryResponse Post(SupportHistoryRequest Shreq)
		{
			SupportHistoryResponse Shr = new SupportHistoryResponse();
			try
			{
					string Sql = @"SELECT 
								id, 
								ticket_id,
								solution_id, 
								field, 
								value, 
								username, 
								field_id, 
								eb_created_at
								FROM support_ticket_history
							WHERE 
								ticket_id =:ticketno 
							AND 
							eb_del=:fals
							;";

					DbParameter[] parameters = {
						this.InfraConnectionFactory.DataDB.GetNewParameter("ticketno", EbDbTypes.String, Shreq.TicketNo),
						this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F")
							};
				
				EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(Sql, parameters);
				
				if (dt.Rows.Count > 0)
				{
					for (int i = 0; i < dt.Rows.Count; i++)
					{
						SupportHistory Sh = new SupportHistory();
						Sh.Id =(int) dt.Rows[i][0];
						Sh.TicketId = dt.Rows[i][1].ToString();
						Sh.SolutionId = dt.Rows[i][2].ToString();
						Sh.Field = dt.Rows[i][3].ToString();
						Sh.Value = dt.Rows[i][4].ToString();
						Sh.UserName = dt.Rows[i][5].ToString();
						Sh.FieldId = (int)dt.Rows[i][6];
						DateTime Dat1 = (DateTime)dt.Rows[i][7];
						Sh.CreatedDate = (Dat1.Date).ToString("d");
						Sh.CreatedTime = Dat1.ToString("HH:mm");

						Shr.SpHistory.Add(Sh);
					}
				}

			}
			catch (Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
				Shr.ErMsg = "Unexpected error occurred";
			}


			return Shr;
		}

		public CommentResponse Post(CommentRequest Cmreq)
		{
			CommentResponse Cm = new CommentResponse();
			try
			{
				string sql = @"INSERT INTO  support_ticket_history(
																	ticket_id,
																	eb_del,
																	field,
																	value,
																	username,
																	field_id,
																	eb_created_at,
																	solution_id

																	)
																	VALUES(
																		:tktid,
																		:fals,
																		:fld,
																		:val,
																		:usrname,
																		:fldid,
																		NOW(),
																		:slid
																		)RETURNING id;";
				DbParameter[] parameters = {
								this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, Cmreq.TicketNo),
								this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
								this.InfraConnectionFactory.DataDB.GetNewParameter("fld", EbDbTypes.String, SupportTicketFields.comment.ToString()),
								this.InfraConnectionFactory.DataDB.GetNewParameter("val", EbDbTypes.String,Cmreq.Comments),
								this.InfraConnectionFactory.DataDB.GetNewParameter("fldid", EbDbTypes.Int32,  SupportTicketFields.comment),
								this.InfraConnectionFactory.DataDB.GetNewParameter("usrname", EbDbTypes.String,Cmreq.UserName),
								this.InfraConnectionFactory.DataDB.GetNewParameter("slid", EbDbTypes.String, Cmreq.Solution_id),
								};

				EbDataTable dt6 = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters);
				var ide = Convert.ToInt32(dt6.Rows[0][0]);

			}
			catch (Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
				Cm.ErMsg = "Unexpected error occurred";
			}

			return Cm;
		}


	}
}
