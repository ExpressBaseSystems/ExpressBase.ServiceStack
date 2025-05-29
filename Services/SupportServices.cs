using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
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
                    //for sending email to expressbase
                    if (dt2 > 0)
                    {
                        SendAlertEmail(sbreq, sbgf);
                    }
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


        //for ticket save 
        public SubmitTicketResponse Post(SubmitTicketRequest streq)
        {
            SubmitTicketResponse st = new SubmitTicketResponse();
            try
            {
                // Insert the new ticket into the database
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
                                    onbehalf,
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
                                    :onbehalf,
                                    :email
                                )RETURNING id,eb_created_at;";

                DbParameter[] parameters = {
            this.InfraConnectionFactory.DataDB.GetNewParameter("usrid", EbDbTypes.Int32, streq.UserId),
            this.InfraConnectionFactory.DataDB.GetNewParameter("usrtyp", EbDbTypes.String, streq.usertype),
            this.InfraConnectionFactory.DataDB.GetNewParameter("title", EbDbTypes.String, streq.title),
            this.InfraConnectionFactory.DataDB.GetNewParameter("descr", EbDbTypes.String, streq.description),
            this.InfraConnectionFactory.DataDB.GetNewParameter("priority", EbDbTypes.String, streq.priority),
            this.InfraConnectionFactory.DataDB.GetNewParameter("solid", EbDbTypes.String, streq.solutionid),
            this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
            this.InfraConnectionFactory.DataDB.GetNewParameter("sts", EbDbTypes.String, streq.status),
            this.InfraConnectionFactory.DataDB.GetNewParameter("typ", EbDbTypes.String, streq.type_b_f),
            this.InfraConnectionFactory.DataDB.GetNewParameter("fname", EbDbTypes.String, streq.fullname),
            this.InfraConnectionFactory.DataDB.GetNewParameter("onbehalf", EbDbTypes.Int32, streq.onBehalfOf),
            this.InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String, streq.email)

        };

                // Execute the insert query and get the generated ticket ID and creation date
                EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters);
                st.Id = Convert.ToInt32(dt.Rows[0][0]);
                string datecrt = dt.Rows[0][1].ToString();

                if (st.Id > 0)
                {
                    // Format the ticket ID to 10 characters with leading zeros
                    string cx = st.Id.ToString();
                    string l = string.Format("SELECT LPAD('{0}', 10, '0');", cx);
                    EbDataTable dt3 = this.InfraConnectionFactory.DataDB.DoQuery(l);
                    string stgf = "IS" + dt3.Rows[0][0]; // Add prefix based on the type

                    // Update the ticket with the formatted ticket ID
                    string k = "UPDATE support_ticket SET ticket_id = :tkt WHERE id = :tktid AND eb_del = :fls;";
                    DbParameter[] param = {
                this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.Int32, st.Id),
                this.InfraConnectionFactory.DataDB.GetNewParameter("fls", EbDbTypes.String, "F"),
                this.InfraConnectionFactory.DataDB.GetNewParameter("tkt", EbDbTypes.String, stgf)
            };
                    int dt2 = this.InfraConnectionFactory.DataDB.DoNonQuery(k, param);

                    // Insert the ticket creation history with the description
                    string sql6 = @"INSERT INTO support_ticket_history(
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
                                                ) RETURNING id;";
                    DbParameter[] parameters6 = {
                this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, stgf),
                this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
                this.InfraConnectionFactory.DataDB.GetNewParameter("fld", EbDbTypes.String, "Description"),
                this.InfraConnectionFactory.DataDB.GetNewParameter("val", EbDbTypes.String, streq.description), // Include description here
                this.InfraConnectionFactory.DataDB.GetNewParameter("fldid", EbDbTypes.Int32, SupportTicketFields.description),
                this.InfraConnectionFactory.DataDB.GetNewParameter("usrname", EbDbTypes.String, streq.fullname),
                this.InfraConnectionFactory.DataDB.GetNewParameter("slid", EbDbTypes.String, streq.solutionid),
            };

                    EbDataTable dt6 = this.InfraConnectionFactory.DataDB.DoQuery(sql6, parameters6);
                    var ide = Convert.ToInt32(dt6.Rows[0][0]);

                    // Upload files associated with the ticket
                    if (streq.Fileuploadlst.Count > 0)
                    {
                        for (var i = 0; i < streq.Fileuploadlst.Count; i++)
                        {
                            string sql3 = @"INSERT INTO support_ticket_files(
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
                                                ) RETURNING id;";
                            DbParameter[] parameters3 = {
                        this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, stgf),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("filebt", EbDbTypes.Bytea, streq.Fileuploadlst[i].Filecollection),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("cnttyp", EbDbTypes.String, streq.Fileuploadlst[i].ContentType),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("flname", EbDbTypes.String, streq.Fileuploadlst[i].FileName),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("slid", EbDbTypes.String, streq.solutionid),
                    };

                            EbDataTable dt4 = this.InfraConnectionFactory.DataDB.DoQuery(sql3, parameters3);
                            var iden = Convert.ToInt32(dt4.Rows[0][0]);
                        }
                    }
                }
                else
                {
                    st.ErMsg = "Error occurred while saving";
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.Message + e.StackTrace);
                st.ErMsg = "Unexpected error occurred";
            }
            return st;
        }


        // for sending alert email to expressbase mail id
        public void SendAlertEmail(SaveBugRequest sbreq, string TktId)
        {
            string msg = string.Format(@"A bug/feature request is raised by user {0} in solution {1} details are as below <br>
Ticket id: {2},<br>
Title: {3},<br>
Description: {4},<br>
Priority: {5},<br>
Type: {6}", sbreq.fullname, sbreq.solutionid, TktId, sbreq.title, sbreq.description, sbreq.priority, sbreq.type_b_f);
            EmailService emailService = base.ResolveService<EmailService>();
            emailService.Post(new EmailDirectRequest
            {
                To = "support@expressbase.com",
                Subject = "Bug/Feature request",
                Message = msg,
                SolnId = sbreq.solutionid
            });
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
                    tr.sol_id.Add(dt.Rows[i][0].ToString());
                    tr.solname.Add(dt.Rows[i][1].ToString());
                    tr.sol_exid.Add(dt.Rows[i][2].ToString());
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.Message + e.StackTrace);
                tr.ErMsg = "Unexpected error occurred";
            }
            return tr;
        }


        public FetchSupportResponse Post(FetchSupportRequest fsreq)
        {
            FetchSupportResponse fr = new FetchSupportResponse();
            try
            {
                DateTime tdate = DateTime.UtcNow;
                string sql = "";
                List<DbParameter> parameters = new List<DbParameter>();

                if (fsreq.WhichConsole.Equals("dc"))
                {
                    sql = @"
                SELECT 
                    ticket_id, 
                    title, 
                    description, 
                    status, 
                    fullname, 
                    eb_created_at 
                FROM support_ticket 
                WHERE solution_id = :sln 
                  AND eb_del = :fls 
                ORDER BY id;";

                    parameters.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("sln", EbDbTypes.String, fsreq.SolnId));
                    parameters.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("fls", EbDbTypes.String, "F"));
                }
                else if (fsreq.WhichConsole.Equals("uc"))
                {
                    sql = @"
                SELECT 
                    ticket_id, 
                    title, 
                    description, 
                    status, 
                    fullname, 
                    eb_created_at 
                FROM support_ticket 
                WHERE solution_id = :sln 
                  AND eb_del = :fls 
                  AND user_type = :utyp
                ORDER BY id;";

                    parameters.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("sln", EbDbTypes.String, fsreq.SolnId));
                    parameters.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("fls", EbDbTypes.String, "F"));
                    parameters.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("utyp", EbDbTypes.String, "user"));
                }

                if (!string.IsNullOrEmpty(sql))
                {
                    EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters.ToArray());

                    for (int i = 0; i < dt.Rows.Count; i++)
                    {
                        TicketLite ticket = new TicketLite
                        {
                            ticketid = dt.Rows[i]["ticket_id"].ToString(),
                            title = dt.Rows[i]["title"].ToString(),
                            description = dt.Rows[i]["description"].ToString(),
                            status = dt.Rows[i]["status"].ToString(),
                            fullname = dt.Rows[i]["fullname"].ToString(),
                            lstmodified = dt.Rows[i]["eb_created_at"].ToString()
                        };

                        if (ticket.status.Equals("Resolved", StringComparison.OrdinalIgnoreCase))
                            fr.ClosedTicket.Add(ticket);
                        else
                            fr.ActiveTicket.Add(ticket);
                    }
                }
                else
                {
                    fr.ErrorMessage = "Invalid console type specified.";
                }
            }
            catch (Exception ex)
            {
                fr.ErrorMessage = ex.Message;
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
									support_ticket.title, 
									support_ticket.description,
									support_ticket.priority, 
									support_ticket.solution_id, 
									support_ticket.eb_created_at, 
									support_ticket.status, 
									support_ticket.remarks, 
									support_ticket.assigned_to, 
									support_ticket.type_bg_fr,
									support_ticket.ticket_id,
									eb_solutions.solution_name,
									eb_solutions.esolution_id	
								FROM 
									support_ticket
								JOIN
									eb_solutions
								ON
									support_ticket.solution_id 
								= 
									eb_solutions.isolution_id 
								WHERE 
									support_ticket.eb_del='F'
								ORDER BY support_ticket.id
								 
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
                        st.NoHour = (tdate - stdate).Hours.ToString();
                        st.NoDays = (tdate - stdate).Days.ToString();
                        st.lstmodified = dt2.Rows[i][4].ToString();
                        st.status = dt2.Rows[i][5].ToString();
                        st.remarks = dt2.Rows[i][6].ToString();
                        st.assignedto = dt2.Rows[i][7].ToString();
                        st.type_b_f = dt2.Rows[i][8].ToString();
                        st.ticketid = dt2.Rows[i][9].ToString();
                        st.Solution_name = dt2.Rows[i][10].ToString();
                        st.Esolution_id = dt2.Rows[i][11].ToString();
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

        // fetch complete details of ticket and show it in edit /view ticket
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
									support_ticket.title, 
									support_ticket.description,
									support_ticket.priority, 
									support_ticket.solution_id, 
									support_ticket.modified_at, 
									support_ticket.status, 
									support_ticket.remarks, 
									support_ticket.assigned_to, 
									support_ticket.type_bg_fr,
									support_ticket.eb_created_at,
									support_ticket.user_type,
									eb_solutions.solution_name,
									eb_solutions.esolution_id	
								FROM support_ticket
								JOIN
									eb_solutions
								ON
									support_ticket.solution_id 
								= 
									eb_solutions.isolution_id 
							WHERE 
								support_ticket.ticket_id =:ticketno 
							AND 
								support_ticket.eb_del=:fals
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
									support_ticket.title, 
									support_ticket.description,
									support_ticket.priority, 
									support_ticket.solution_id, 
									support_ticket.modified_at, 
									support_ticket.status, 
									support_ticket.remarks, 
									support_ticket.assigned_to, 
									support_ticket.type_bg_fr,
									support_ticket.eb_created_at,
									support_ticket.user_type,									
									eb_solutions.solution_name,
									eb_solutions.esolution_id	
									FROM 
										support_ticket
									JOIN
										eb_solutions
									ON
										support_ticket.solution_id 
									= 
										eb_solutions.isolution_id 
								WHERE 
									support_ticket.ticket_id =:ticketno
								AND 
									support_ticket.eb_del=:fals 
								AND 
									support_ticket.solution_id
								IN
								(SELECT 
										eb_solutions.isolution_id 
									FROM
										eb_solutions
									WHERE 
										eb_solutions.tenant_id =:UserId
									AND 
										eb_solutions.eb_del = false);";


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
												support_ticket.title, 
												support_ticket.description,
												support_ticket.priority, 
												support_ticket.solution_id, 
												support_ticket.modified_at, 
												support_ticket.status, 
												support_ticket.remarks, 
												support_ticket.assigned_to, 
												support_ticket.type_bg_fr,
												support_ticket.eb_created_at,
												support_ticket.user_type,
												eb_solutions.solution_name,
												eb_solutions.esolution_id	
											FROM 
												support_ticket
											JOIN
												eb_solutions
											ON
												support_ticket.solution_id 
											= 
												eb_solutions.isolution_id 
											WHERE 
												support_ticket.ticket_id =:ticketno 
											AND
												support_ticket.eb_del=:fals 
											AND
												support_ticket.solution_id=:SolnId;");

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
                        st.Solution_name = dt.Rows[i][11].ToString();
                        st.Esolution_id = dt.Rows[i][12].ToString();

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

        public GetTicketByIdResponse Get(GetTicketByIdRequest request)
        {
            var response = new GetTicketByIdResponse();
            try
            {
                string sql = @"SELECT 
             support_ticket.title, 
             support_ticket.description,
             support_ticket.priority, 
             support_ticket.solution_id, 
             support_ticket.eb_created_at, 
             support_ticket.status, 
             support_ticket.remarks, 
             support_ticket.assigned_to, 
             support_ticket.type_bg_fr,
             support_ticket.ticket_id,
             support_ticket.fullname,
             support_ticket.onbehalf,
             support_ticket.eta,
             support_ticket.estimated_hours,
             support_ticket.actual_hours,
             eb_solutions.solution_name,
             eb_solutions.esolution_id
         FROM support_ticket
         JOIN eb_solutions
           ON support_ticket.solution_id = eb_solutions.isolution_id 
         WHERE support_ticket.ticket_id = :ticketId 
           AND support_ticket.eb_del = 'F'
         LIMIT 1;";

                DbParameter[] parameters = {
         this.InfraConnectionFactory.DataDB.GetNewParameter("ticketId", EbDbTypes.String, request.TicketId)
     };

                EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters);

                if (dt.Rows.Count > 0)
                {
                    var row = dt.Rows[0];
                    SupportTktCls st = new SupportTktCls
                    {
                        title = row[0].ToString(),
                        description = row[1].ToString(),
                        priority = row[2].ToString(),
                        solutionid = row[3].ToString(),
                        lstmodified = row[4].ToString(),
                        NoHour = (DateTime.UtcNow - (DateTime)row[4]).Hours.ToString(),
                        NoDays = (DateTime.UtcNow - (DateTime)row[4]).Days.ToString(),
                        status = row[5].ToString(),
                        remarks = row[6].ToString(),
                        assignedto = row[7].ToString(),
                        type_b_f = row[8].ToString(),
                        ticketid = row[9].ToString(),
                        fullname = row[10].ToString(),
                        onbehalf = row[11].ToString(),
                        eta = row[12].ToString(),
                        estimated_hours = row[13].ToString(),
                        actual_hours = row[14].ToString(),
                        Solution_name = row[15].ToString(),
                        Esolution_id = row[16].ToString()
                    };

                    // Fetch files
                    string fileSql = @"SELECT 
                             file_name, 
                             content_type, 
                             img_bytea, 
                             id
                           FROM support_ticket_files
                           WHERE ticket_id = :ticketId AND eb_del = 'F';";

                    DbParameter[] fileParams = {
             this.InfraConnectionFactory.DataDB.GetNewParameter("ticketId", EbDbTypes.String, request.TicketId)
         };

                    EbDataTable fileDt = this.InfraConnectionFactory.DataDB.DoQuery(fileSql, fileParams);
                    foreach (var fileRow in fileDt.Rows)
                    {
                        FileUploadCls file = new FileUploadCls
                        {
                            FileName = fileRow["file_name"].ToString(),
                            ContentType = fileRow["content_type"].ToString(),
                            Filecollection = (byte[])fileRow["img_bytea"],
                            FileId = Convert.ToInt32(fileRow["id"])
                        };
                        st.Fileuploadlst.Add(file);
                    }

                    response.Ticket = st;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error in GetTicketById: " + ex.Message);
            }

            return response;
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

                string[] DBcolms = new string[] { "title", "description", "priority", "solution_id", "type_bg_fr", "assigned_to", "status", "comment", "files", "date_created", "eta", "estimated_hours", "actual_hours" };

                if (utreq.chngedtkt.Count > 0)
                {
                    for (int i = 0; i < DBcolms.Length; i++)
                    {
                        if (utreq.chngedtkt.ContainsKey(DBcolms[i]))
                        {
                            var value = utreq.chngedtkt[DBcolms[i]];
                            if (string.IsNullOrEmpty(value)) continue; // skip empty values

                            EbDbTypes dbType = EbDbTypes.String;

                            switch (DBcolms[i])
                            {
                                case "eta":
                                    dbType = EbDbTypes.DateTime;
                                    break;
                                case "estimated_hours":
                                case "actual_hours":
                                    dbType = EbDbTypes.Decimal;
                                    break;
                                case "status":
                                    dbType = EbDbTypes.String; // Ensure 'status' is treated as a string, adjust if needed
                                    break; 
                            }

                            FieldKey.Add(DBcolms[i]);
                            FieldValue.Add(":" + DBcolms[i]);
                            p.Add(this.InfraConnectionFactory.DataDB.GetNewParameter(":" + DBcolms[i], dbType, value));
                        }
                    }//

                    for (int j = 0; j < FieldKey.Count; j++)
                    {
                        tem += FieldKey[j] + "=" + FieldValue[j] + ",";
                    }

                    tem = tem.Remove(tem.Length - 1, 1);
                    string k = $@"UPDATE support_ticket 
                          SET {tem}
                          WHERE ticket_id = :tktid AND eb_del = :fals";

                    p.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, utreq.ticketid));
                    p.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"));
                    DbParameter[] parameters = p.ToArray();
                    int dt = this.InfraConnectionFactory.DataDB.DoNonQuery(k, parameters);

                    if (dt == 0)
                    {
                        utr.ErMsg = "Unexpected error occurred while updating";
                    }
                }

                // Handle File Deletion (Remove Previously Uploaded Files)
                if (utreq.Filedel != null && utreq.Filedel.Length > 0)
                {
                    foreach (var fileId in utreq.Filedel)
                    {
                        string deleteFileSql = @"UPDATE support_ticket_files 
                                         SET eb_del = :tru 
                                         WHERE ticket_id = :tktid AND eb_del = :fals AND id = :fileid";

                        DbParameter[] deleteParams = {
                    this.InfraConnectionFactory.DataDB.GetNewParameter("tru", EbDbTypes.String, "T"),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, utreq.ticketid),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("fileid", EbDbTypes.Int32, fileId)
                };

                        int deleteResult = this.InfraConnectionFactory.DataDB.DoNonQuery(deleteFileSql, deleteParams);
                        if (deleteResult < 1)
                        {
                            utr.ErMsg = "Unexpected error occurred while deleting a file";
                        }
                    }
                }

                // Handle File Upload (Check for Duplicates & Update Existing Files)
                if (utreq.Fileuploadlst.Count > 0)
                {
                    foreach (var file in utreq.Fileuploadlst)
                    {
                        // Check if file already exists (Match Name, Content Type, AND Data)
                        string checkFileSql = @"SELECT id FROM support_ticket_files 
                                        WHERE ticket_id = :tktid 
                                        AND file_name = :flname 
                                        AND content_type = :cnttyp 
                                        AND img_bytea = :filedata 
                                        AND eb_del = :fals";

                        DbParameter[] checkFileParams = {
                    this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, utreq.ticketid),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("flname", EbDbTypes.String, file.FileName),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("cnttyp", EbDbTypes.String, file.ContentType),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("filedata", EbDbTypes.Bytea, file.Filecollection),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F")
                };

                        EbDataTable existingFileCheck = this.InfraConnectionFactory.DataDB.DoQuery(checkFileSql, checkFileParams);

                        if (existingFileCheck.Rows.Count > 0)
                        {
                            // If the file already exists, update it instead of inserting
                            int existingFileId = Convert.ToInt32(existingFileCheck.Rows[0]["id"]);

                            string updateFileSql = @"UPDATE support_ticket_files 
                                             SET img_bytea = :filebt, content_type = :cnttyp 
                                             WHERE id = :fileid AND ticket_id = :tktid";

                            DbParameter[] updateParams = {
                        this.InfraConnectionFactory.DataDB.GetNewParameter("filebt", EbDbTypes.Bytea, file.Filecollection),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("cnttyp", EbDbTypes.String, file.ContentType),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("fileid", EbDbTypes.Int32, existingFileId),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, utreq.ticketid),
                    };

                            this.InfraConnectionFactory.DataDB.DoQuery(updateFileSql, updateParams);
                        }
                        else
                        {
                            // Insert a new file only if it's completely new
                            string insertFileSql = @"INSERT INTO support_ticket_files(
                                                ticket_id, eb_del, img_bytea, content_type, file_name, solution_id)
                                            VALUES (:tktid, :fals, :filebt, :cnttyp, :flname, :solu) RETURNING id;";

                            DbParameter[] insertParams = {
                        this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, utreq.ticketid),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("filebt", EbDbTypes.Bytea, file.Filecollection),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("cnttyp", EbDbTypes.String, file.ContentType),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("flname", EbDbTypes.String, file.FileName),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("solu", EbDbTypes.String, utreq.solution_id),
                    };

                            this.InfraConnectionFactory.DataDB.DoQuery(insertFileSql, insertParams);
                        }
                    }
                }

                utr.status = true;
            }
            catch (Exception e)
            {
                utr.ErMsg = "Unexpected error occurred: " + e.Message;
            }

            return utr;
        }


        public UpdateTicketAdminResponse Post(UpdateTicketAdminRequest utreq)
        {
            UpdateTicketAdminResponse utr = new UpdateTicketAdminResponse();
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
                                            and eb_del=:fals
											and solution_id = :soluid", tem
                                            );

                p.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, utreq.Ticketid));
                p.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"));
                p.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("soluid", EbDbTypes.String, utreq.Solution_id));
                DbParameter[] parameters = p.ToArray();
                int dt = this.InfraConnectionFactory.DataDB.DoNonQuery(k, parameters);

                if (dt == 0)
                {
                    utr.ErMsg = "Unexpected error occurred while updating";
                }
                else
                {
                    //insert into history
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
                            this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, utreq.Ticketid),
                            this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
                            this.InfraConnectionFactory.DataDB.GetNewParameter("fld", EbDbTypes.String,klist[j]),
                            this.InfraConnectionFactory.DataDB.GetNewParameter("val", EbDbTypes.String,vlist[j] ),
                            this.InfraConnectionFactory.DataDB.GetNewParameter("fldid", EbDbTypes.Int32,  (int)SupportTicketFields.status),
                            this.InfraConnectionFactory.DataDB.GetNewParameter("usrname", EbDbTypes.String, utreq.usrname),
                            this.InfraConnectionFactory.DataDB.GetNewParameter("nwtime", EbDbTypes.DateTime, tdate),
                            this.InfraConnectionFactory.DataDB.GetNewParameter("slid", EbDbTypes.String, utreq.Solution_id),
                            };

                        EbDataTable dt6 = this.InfraConnectionFactory.DataDB.DoQuery(sql6, parameters6);
                        var ide = Convert.ToInt32(dt6.Rows[0][0]);
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
                //insert into history
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
                    DbParameter[] parameters7 = {
                            this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String,  chstreq.TicketNo),
                            this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
                            this.InfraConnectionFactory.DataDB.GetNewParameter("fld", EbDbTypes.String,SupportTicketFields.reason.ToString()),
                            this.InfraConnectionFactory.DataDB.GetNewParameter("val", EbDbTypes.String,chstreq.Reason ),
                            this.InfraConnectionFactory.DataDB.GetNewParameter("fldid", EbDbTypes.Int32,  (int)SupportTicketFields.reason),
                            this.InfraConnectionFactory.DataDB.GetNewParameter("usrname", EbDbTypes.String, chstreq.UserName),
                            this.InfraConnectionFactory.DataDB.GetNewParameter("tdate", EbDbTypes.DateTime, tdate),
                            this.InfraConnectionFactory.DataDB.GetNewParameter("slid", EbDbTypes.String, chstreq.Solution_id),
                                };
                    EbDataTable dt6 = this.InfraConnectionFactory.DataDB.DoQuery(sql6, parameters6);
                    EbDataTable dt7 = this.InfraConnectionFactory.DataDB.DoQuery(sql6, parameters7);
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
                        Sh.Id = (int)dt.Rows[i][0];
                        Sh.TicketId = dt.Rows[i][1].ToString();
                        Sh.SolutionId = dt.Rows[i][2].ToString();
                        Sh.Field = dt.Rows[i][3].ToString();
                        Sh.Value = dt.Rows[i][4].ToString();
                        Sh.UserName = dt.Rows[i][5].ToString();
                        Sh.FieldId = (int)dt.Rows[i][6];
                        DateTime Dat1 = (DateTime)dt.Rows[i][7];
                        Dat1 = Dat1.ConvertFromUtc(Shreq.UserObject.Preference.TimeZone);
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
                string sql = @"INSERT INTO support_ticket_history(
                            ticket_id,
                            eb_del,
                            field,
                            ""value"",
                            username,
                            field_id,
                            eb_created_at,
                            solution_id,
                            currentUserid
                        )
                        VALUES(
                            :tktid,
                            :fals,
                            :fld,
                            :val,
                            :usrname,
                            :fldid,
                            NOW(),
                            :slid,
                            :uid
                        ) RETURNING id;";

                DbParameter[] parameters = {
            this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, Cmreq.TicketNo),
            this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
            this.InfraConnectionFactory.DataDB.GetNewParameter("fld", EbDbTypes.String, SupportTicketFields.comment.ToString()),
            this.InfraConnectionFactory.DataDB.GetNewParameter("val", EbDbTypes.String, Cmreq.Comments),
            this.InfraConnectionFactory.DataDB.GetNewParameter("fldid", EbDbTypes.Int32, SupportTicketFields.comment),
            this.InfraConnectionFactory.DataDB.GetNewParameter("usrname", EbDbTypes.String, Cmreq.UserName),
            this.InfraConnectionFactory.DataDB.GetNewParameter("slid", EbDbTypes.String, Cmreq.Solution_id),
            this.InfraConnectionFactory.DataDB.GetNewParameter("uid", EbDbTypes.String, Cmreq.currentUserid),

        };

                EbDataTable dt6 = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters);

                if (dt6.Rows.Count > 0)
                {
                    Cm.CmntStatus = true;
                }
                else
                {
                    Cm.ErMsg = "Insert failed: No data returned";
                }
            }
            catch (Exception e)
            {
                Cm.ErMsg = "Unexpected error occurred: " + e.Message;
                Console.WriteLine("Exception in Post(): " + e.ToString());
            }

            return Cm;
        }

        public CommentListResponse Post(CommentListRequest req)
        {
            var response = new CommentListResponse();

            try
            {
                string sql = @"SELECT username, ""value"", eb_created_at, currentUserid 
                       FROM support_ticket_history 
                       WHERE ticket_id = :tktid AND field = 'comment' AND eb_del = 'F'
                       ORDER BY eb_created_at ASC";

                DbParameter[] parameters = {
            this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, req.TicketNo)
        };

                EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters);

                foreach (EbDataRow row in dt.Rows)
                {
                    string username = row["username"]?.ToString();
                    string commentText = row["value"]?.ToString();
                    string createdAtStr = row["eb_created_at"]?.ToString();
                    string userId = row["currentUserid"]?.ToString();


                    // Debug print
                    Console.WriteLine($"username: {username}, value: {commentText}, eb_created_at: {createdAtStr}");

                    var comment = new CommentDto
                    {
                        UserName = username ?? "[Unknown]",
                        CommentText = commentText ?? "[No Comment]",
                        CreatedAt = DateTime.TryParse(createdAtStr, out var parsedDate) ? parsedDate : DateTime.MinValue,
                         UserId = userId ?? ""

                    };


                    response.Comments.Add(comment);
                }

                response.Success = true;
            }
            catch (Exception ex)
            {
                response.Success = false;
                response.ErrorMessage = "Error loading comments: " + ex.Message;
                Console.WriteLine("Fetch Exception: " + ex);
            }

            return response;
        }


    }
}
