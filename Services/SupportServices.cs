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
                                                )RETURNING id;";

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

				if (sb.Id > 0)
				{
					string cx = sb.Id.ToString();

					//for making id 6 digit with intial position 0 in case of single digit

					string l = string.Format("select lpad('{0}',6,'0');", cx);
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
					string k = String.Format("UPDATE support_ticket SET bg_fr_id = :bfi WHERE id={0} and eb_del='F';", sb.Id);
					DbParameter[] param = {
					this.InfraConnectionFactory.DataDB.GetNewParameter("bfi", EbDbTypes.String,sbgf)
					};
					int dt2 = this.InfraConnectionFactory.DataDB.DoNonQuery(k, param);

					//to upload images
					FileUploadCls flupcl = new FileUploadCls();
					if (sbreq.Fileuploadlst.Count > 0)
					{
						for (var i = 0; i < sbreq.Fileuploadlst.Count; i++)
						{
							byte[] sa = sbreq.Fileuploadlst[i].Filecollection;

							string sql3 = @"INSERT INTO  support_ticket_files(
																	ticket_id,
																	bg_fr_id,
																	eb_del,
																	img_bytea,
																	content_type,
																	file_name,
																	solution_id

																	)
																	VALUES(
																	:tktid,
																	:bgid,
																	:fals,
																	:filebt,
																	:cnttyp,
																	:flname,
																	:slid
																	)RETURNING id;";
							DbParameter[] parameters3 = {
								this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.Int32, sb.Id),
								this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
								this.InfraConnectionFactory.DataDB.GetNewParameter("bgid", EbDbTypes.String, sbgf),
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
			}
			catch (Exception e)
			{
				Console.WriteLine("Exception: " + e.Message + e.StackTrace);
			}
			return sb;
		}



		//to fetch solution id,name from tenant table  to show in dropdown

		public TenantSolutionsResponse Post(TenantSolutionsRequest tsreq)
		{
			TenantSolutionsResponse tr = new TenantSolutionsResponse();

			try
			{
				string sql = string.Format("SELECT isolution_id,solution_name,esolution_id  FROM eb_solutions WHERE tenant_id={0} AND eb_del='F';", tsreq.UserId);

				EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql);


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
			}
			return tr;
		}


		//to fetch all details of tickets of corresponding user of that corresponding solution to show as tables
		public FetchSupportResponse Post(FetchSupportRequest fsreq)
		{
			FetchSupportResponse fr = new FetchSupportResponse();
			try
			{

				if (fsreq.WhichConsole.Equals("tc"))
				{
					string sql2 = @"SELECT 
								title, 
								description,
								priority, 
								solution_id, 
								modified_at, 
								status, 
								remarks, 
								assigned_to, 
								type_bg_fr,
								bg_fr_id
								FROM support_ticket
								WHERE 
								eb_del='F' 
								AND 
								solution_id 
								IN
								(SELECT isolution_id  FROM eb_solutions WHERE tenant_id=:tndid AND eb_del='F');";
					DbParameter[] parameters2 = {
					this.InfraConnectionFactory.DataDB.GetNewParameter("tndid", EbDbTypes.Int32, fsreq.UserId)
					};

					EbDataTable dt2 = this.InfraConnectionFactory.DataDB.DoQuery(sql2, parameters2);
					for (int i = 0; i < dt2.Rows.Count; i++)
					{
						SupportTktCls st = new SupportTktCls();
						st.title = dt2.Rows[i][0].ToString();
						st.description = dt2.Rows[i][1].ToString();
						st.priority = dt2.Rows[i][2].ToString();
						st.solutionid = dt2.Rows[i][3].ToString();
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
					string sql = @"SELECT 
								title, 
								description,
								priority, 
								solution_id, 
								modified_at, 
								status, 
								remarks, 
								assigned_to, 
								type_bg_fr,
								bg_fr_id
								FROM support_ticket
								WHERE 
								eb_created_by =:usr AND solution_id =:sln AND eb_del=:fls;";
					DbParameter[] parameters3 = {
					this.InfraConnectionFactory.DataDB.GetNewParameter("usr", EbDbTypes.Int32, fsreq.UserId),
					this.InfraConnectionFactory.DataDB.GetNewParameter("sln", EbDbTypes.String, fsreq.SolnId),
					this.InfraConnectionFactory.DataDB.GetNewParameter("fls", EbDbTypes.String, "F")
					};

					EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters3);
					for (int i = 0; i < dt.Rows.Count; i++)
					{
						SupportTktCls st = new SupportTktCls();
						st.title = dt.Rows[i][0].ToString();
						st.description = dt.Rows[i][1].ToString();
						st.priority = dt.Rows[i][2].ToString();
						st.solutionid = dt.Rows[i][3].ToString();
						st.lstmodified = dt.Rows[i][4].ToString();
						st.status = dt.Rows[i][5].ToString();
						st.remarks = dt.Rows[i][6].ToString();
						st.assignedto = dt.Rows[i][7].ToString();
						st.type_b_f = dt.Rows[i][8].ToString();
						st.ticketid = dt.Rows[i][9].ToString();
						fr.supporttkt.Add(st);
					}
				}

			}
			catch (Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
			}
			return fr;
		}

		//to fetch all details of tickets of corresponding user of that corresponding solution to show as tables of admin solution
		public AdminSupportResponse Post(AdminSupportRequest asreq)
		{
			AdminSupportResponse asr = new AdminSupportResponse();
			try
			{
				string sql2 = @"SELECT 
								title, 
								description,
								priority, 
								solution_id, 
								modified_at, 
								status, 
								remarks, 
								assigned_to, 
								type_bg_fr,
								bg_fr_id
								FROM support_ticket
								WHERE 
								eb_del='F' ORDER BY modified_at 
								;";
				EbDataTable dt2 = this.InfraConnectionFactory.DataDB.DoQuery(sql2);
				for (int i = 0; i < dt2.Rows.Count; i++)
				{
					SupportTktCls st = new SupportTktCls();
					st.title = dt2.Rows[i][0].ToString();
					st.description = dt2.Rows[i][1].ToString();
					st.priority = dt2.Rows[i][2].ToString();
					st.solutionid = dt2.Rows[i][3].ToString();
					st.lstmodified = dt2.Rows[i][4].ToString();
					st.status = dt2.Rows[i][5].ToString();
					st.remarks = dt2.Rows[i][6].ToString();
					st.assignedto = dt2.Rows[i][7].ToString();
					st.type_b_f = dt2.Rows[i][8].ToString();
					st.ticketid = dt2.Rows[i][9].ToString();
					asr.supporttkt.Add(st);
				}

			}
			catch (Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
			}
			return asr;
		}

			// fectch complete details of ticket and show it in edit /view ticket
			public SupportDetailsResponse Post(SupportDetailsRequest sdreq)
		{
			SupportDetailsResponse sd = new SupportDetailsResponse();
			string sql = null;
			string sql1 = null;
			try
			{
				if (sdreq.SolnId.Equals("admin"))
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
								FROM support_ticket
								WHERE 
								bg_fr_id ='{0}' AND eb_del='F';", sdreq.ticketno);

					sql1 = string.Format(@"SELECT id,img_bytea,content_type,file_name from support_ticket_files where bg_fr_id ='{0}' AND eb_del='F' ;", sdreq.ticketno);
				}
				else
				{
					if (sdreq.Usertype.Equals("tc"))
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
								FROM support_ticket
								WHERE 
								bg_fr_id ='{0}' AND eb_del='F' AND solution_id
								IN
								(SELECT isolution_id  FROM eb_solutions WHERE tenant_id ={1} AND eb_del = 'F');", sdreq.ticketno, sdreq.UserId);

						sql1 = string.Format(@"SELECT id,img_bytea,content_type,file_name from support_ticket_files where bg_fr_id ='{0}' AND eb_del='F' AND solution_id
								IN
								(SELECT isolution_id  FROM eb_solutions WHERE tenant_id ={1} AND eb_del = 'F');", sdreq.ticketno, sdreq.UserId);
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
								FROM support_ticket
								WHERE 
								bg_fr_id ='{0}' AND eb_del='F' AND solution_id='{1}';", sdreq.ticketno, sdreq.SolnId);

						sql1 = string.Format(@"SELECT id,img_bytea,content_type,file_name from support_ticket_files where bg_fr_id ='{0}' AND eb_del='F' AND solution_id='{1}';", sdreq.ticketno, sdreq.SolnId);
					}
				}
					

					EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql);
					SupportTktCls st = new SupportTktCls();
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

					EbDataTable dt2 = this.InfraConnectionFactory.DataDB.DoQuery(sql1);

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
						flupcls.FileDataURL = string.Format("data:image/png;base64,{0}", fileBase64Data);

						st.Fileuploadlst.Add(flupcls);
					}
					sd.supporttkt.Add(st);
				
			}
			catch (Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
			}
			return sd;
		}



		public UpdateTicketResponse Post(UpdateTicketRequest utreq)
		{
			UpdateTicketResponse utr = new UpdateTicketResponse();
			utr.status = false;
			try
			{
				string k = String.Format(@"UPDATE 
										support_ticket 
										SET
										title = :titl, 
										description = :descr,
										priority = :prior,
										solution_id = :soluid,
										type_bg_fr=:typ
										WHERE 
											bg_fr_id=:bg_id
                                            and eb_del=:fals"
											);
				DbParameter[] parameters = {
					this.InfraConnectionFactory.DataDB.GetNewParameter("bg_id", EbDbTypes.String, utreq.ticketid),
					this.InfraConnectionFactory.DataDB.GetNewParameter("titl", EbDbTypes.String, utreq.title),
					this.InfraConnectionFactory.DataDB.GetNewParameter("descr", EbDbTypes.String, utreq.description),
					this.InfraConnectionFactory.DataDB.GetNewParameter("prior", EbDbTypes.String, utreq.priority),
					this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
					this.InfraConnectionFactory.DataDB.GetNewParameter("soluid", EbDbTypes.String, utreq.solution_id),
					this.InfraConnectionFactory.DataDB.GetNewParameter("typ", EbDbTypes.String, utreq.type_f_b)

					};
				int dt = this.InfraConnectionFactory.DataDB.DoNonQuery(k, parameters);

				//remove previouse upload files ie set false
				if(utreq.Filedel.Length>0)
				{
					for(var m=0;m< utreq.Filedel.Length; m++)
					{
						string k1 = String.Format(@"UPDATE 
										support_ticket_files 
										SET
										eb_del=:tru 
										WHERE 
											bg_fr_id=:bgid
                                            and eb_del=:fals
											and id=:fileid"
											);


						DbParameter[] parameters5 = {
								this.InfraConnectionFactory.DataDB.GetNewParameter("tru", EbDbTypes.String, "T"),
								this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
								this.InfraConnectionFactory.DataDB.GetNewParameter("bgid", EbDbTypes.String, utreq.ticketid),
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
																	bg_fr_id,
																	eb_del,
																	img_bytea,
																	content_type,
																	file_name
																	)
																	VALUES(
																	:bgid,
																	:fals,
																	:filebt,
																	:cnttyp,
																	:flname
																	)RETURNING id;";
						DbParameter[] parameters3 = {
								this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
								this.InfraConnectionFactory.DataDB.GetNewParameter("bgid", EbDbTypes.String, utreq.ticketid),
								this.InfraConnectionFactory.DataDB.GetNewParameter("filebt", EbDbTypes.Bytea,utreq.Fileuploadlst[i].Filecollection),
								this.InfraConnectionFactory.DataDB.GetNewParameter("cnttyp", EbDbTypes.String, utreq.Fileuploadlst[i].ContentType),
								this.InfraConnectionFactory.DataDB.GetNewParameter("flname", EbDbTypes.String, utreq.Fileuploadlst[i].FileName),
								};

						EbDataTable dt4 = this.InfraConnectionFactory.DataDB.DoQuery(sql3, parameters3);
						var iden = Convert.ToInt32(dt4.Rows[0][0]);

					}
				}





				utr.status = true;



			}
			catch (Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
			}
			return utr;
		}

		public ChangeStatusResponse Post(ChangeStatusRequest chstreq)
		{
			ChangeStatusResponse chst = new ChangeStatusResponse();
			try
			{
				string k = String.Format(@"UPDATE 
										support_ticket 
										SET
										status = :sts
										WHERE 
											bg_fr_id=:bg_id
                                            and eb_del=:fals"
											);
				DbParameter[] parameters = {
					this.InfraConnectionFactory.DataDB.GetNewParameter("bg_id", EbDbTypes.String, chstreq.TicketNo),
					this.InfraConnectionFactory.DataDB.GetNewParameter("sts", EbDbTypes.String,chstreq.NewStatus ),
					this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),

					};
				int dt = this.InfraConnectionFactory.DataDB.DoNonQuery(k, parameters);
				chst.RtnStatus = true;

			}
			catch (Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
			}
			return chst;
		}

	}
}
