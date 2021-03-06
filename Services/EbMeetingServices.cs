﻿using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.LocationNSolution;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Security;
using ExpressBase.Objects;
using ExpressBase.Objects.Objects;
using ExpressBase.Objects.Objects.DVRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Objects.WebFormRelated;
using Jurassic;
using Jurassic.Library;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using Newtonsoft.Json;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using ExpressBase.ServiceStack.MQServices;
using static ExpressBase.Objects.EbMeetingScheduler;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
    public class EbMeetingServices : EbBaseService
    {
        public EbMeetingServices(IEbConnectionFactory _dbf, IMessageProducer _mqp) : base(_dbf, _mqp) { }


        public GetMeetingSlotsResponse Post(GetMeetingSlotsRequest request)
        {
            GetMeetingSlotsResponse Slots = new GetMeetingSlotsResponse();
            string _qry = @"
        SELECT 
		A.id,A.max_attendees,A.Max_hosts, A.no_of_attendee, A.no_of_hosts,A.title , A.description ,A.venue, A.integration,A.duration,
		B.id as slot_id , B.eb_meeting_schedule_id,  B.is_approved, 
		B.meeting_date, B.time_from, B.time_to,
	
		COALESCE (C.slot_host, 0) as slot_host_count,
		COALESCE (C.slot_host_attendee, 0) as slot_attendee_count,
	    COALESCE (D.id, 0) as meeting_id	
		FROM	
			(SELECT 
						id, no_of_attendee, no_of_hosts , max_attendees, max_hosts, title , description , venue, integration ,duration
					FROM  
						eb_meeting_schedule 
					WHERE 
						eb_del = 'F' AND id = 1 )A
				LEFT JOIN
					(SELECT 
							id, eb_meeting_schedule_id , is_approved, 
		                        meeting_date, time_from, time_to 
	                        FROM 
		                        eb_meeting_slots 
	                        WHERE 
		                        eb_del = 'F' AND meeting_date='{1}' )B 
                        ON B.eb_meeting_schedule_id	= A.id 
                        LEFT JOIN 
                        (SELECT 
		                        eb_meeting_schedule_id,approved_slot_id ,type_of_user, COUNT(approved_slot_id)filter(where participant_type = 1) as slot_host,
						 		COUNT(approved_slot_id)filter(where participant_type = 2) as slot_host_attendee
	                        FROM 
		                        eb_meeting_slot_participants
	                        GROUP BY
		                        eb_meeting_schedule_id, approved_slot_id, type_of_user, eb_del
	                        Having
		                        eb_del = 'F')C	
                        ON
 	                        C.eb_meeting_schedule_id = A.id and C.approved_slot_id = B.id
	
                        LEFT JOIN 
                        (SELECT 
		                        id, eb_meeting_slots_id
	                        FROM 
		                        eb_meetings
	                        where
		                        eb_del = 'F') D
		                        ON
 	                        D.eb_meeting_slots_id = B.id
		ORDER BY slot_id
";
            String _query = string.Format(_qry, request.MeetingScheduleId, request.Date);
            try
            {
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(_query);
                int capacity1 = dt.Rows.Count;
                for (int i = 0; i < capacity1; i++)
                {
                    Slots.AllSlots.Add(
                        new SlotProcess()
                        {
                            Meeting_Id = Convert.ToInt32(dt.Rows[i]["id"]),
                            Slot_id = Convert.ToInt32(dt.Rows[i]["slot_id"]),
                            Meeting_schedule_id = Convert.ToInt32(dt.Rows[i]["eb_meeting_schedule_id"]),
                            Title = Convert.ToString(dt.Rows[i]["title"]),
                            Description = Convert.ToString(dt.Rows[i]["description"]),
                            Is_approved = Convert.ToString(dt.Rows[i]["is_approved"]),
                            Date = Convert.ToString(dt.Rows[i]["date"]),
                            Time_from = Convert.ToString(dt.Rows[i]["time_from"]),
                            Time_to = Convert.ToString(dt.Rows[i]["time_to"]),
                            Venue = Convert.ToString(dt.Rows[i]["venue"]),
                            Integration = Convert.ToString(dt.Rows[i]["integration"]),
                            No_Host = Convert.ToInt32(dt.Rows[i]["no_of_hosts"]),
                            No_Attendee = Convert.ToInt32(dt.Rows[i]["no_of_attendee"]),
                            Max_Attendee = Convert.ToInt32(dt.Rows[i]["max_attendees"]),
                            Max_Host = Convert.ToInt32(dt.Rows[i]["max_hosts"]),
                            SlotAttendeeCount = Convert.ToInt32(dt.Rows[i]["slot_attendee_count"]),
                            SlotHostCount = Convert.ToInt32(dt.Rows[i]["slot_participant_count"]),
                            MeetingId = Convert.ToInt32(dt.Rows[i]["slot_participant_count"]),
                            Duration = Convert.ToInt32(dt.Rows[i]["duration"]),
                        });
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message, e.StackTrace);
            }
            return Slots;
        }

        public MeetingSaveValidateResponse Post(MeetingSaveValidateRequest request)
        {
            MeetingSaveValidateResponse Resp = new MeetingSaveValidateResponse();
            string query = @" 
            SELECT 
		     A.id as slot_id , A.eb_meeting_schedule_id, A.is_approved,
			 B.no_of_attendee, B.no_of_hosts,
			 COALESCE (C.slot_host, 0) as slot_host_count,
		     COALESCE (C.slot_host_attendee, 0) as slot_attendee_count,
			 COALESCE (D.id, 0) as meeting_id,
			 COALESCE (E.id, 0) as participant_id
	            FROM
				(SELECT 
						id, eb_meeting_schedule_id , is_approved, 
					meeting_date, time_from, time_to
	                     FROM 
		                     eb_meeting_slots 
	                     WHERE 
		                     eb_del = 'F' and id = {0})A
						LEFT JOIN	 
							 (SELECT id, no_of_attendee, no_of_hosts FROM  eb_meeting_schedule)B
							 ON
 	                     B.id = A.eb_meeting_schedule_id
						LEFT JOIN	
						(SELECT 
		                     eb_meeting_schedule_id,approved_slot_id ,type_of_user, COUNT(approved_slot_id)filter(where type_of_user = 1) as slot_host,
						 		                    COUNT(approved_slot_id)filter(where type_of_user = 2) as slot_host_attendee
	                     FROM 
		                     eb_meeting_slot_participants
	                     GROUP BY
		                     eb_meeting_schedule_id, approved_slot_id, type_of_user, eb_del
	                     Having
		                     eb_del = 'F')C	
                     ON
 	                     C.eb_meeting_schedule_id = B.id and C.approved_slot_id = A.id
                     LEFT JOIN 
                     (SELECT 
		                     id, eb_meeting_slots_id
	                     FROM 
		                     eb_meetings
	                     where
		                     eb_del = 'F') D
		                     ON
 	                     D.eb_meeting_slots_id = A.id
						 LEFT JOIN (
						 SELECT id , eb_meeting_schedule_id , approved_slot_id ,type_of_user, COUNT(approved_slot_id)filter(where type_of_user = 1) as slot_host,
					COUNT(approved_slot_id)filter(where type_of_user = 2) as slot_host_attendee
	                     FROM 
		                     eb_meeting_slot_participants
							  GROUP BY
		                        eb_meeting_schedule_id, approved_slot_id, type_of_user, eb_del , id)E
								  ON
 	                     E.approved_slot_id = A.id ; 

                                        ";
            List<DetailsBySlotid> SlotObj = new List<DetailsBySlotid>();
            bool Status = false;
            try
            {
                String _query = string.Format(query, request.SlotParticipant.ApprovedSlotId); ;
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(_query);
                int capacity1 = dt.Rows.Count;
                for (int i = 0; i < capacity1; i++)
                {
                    SlotObj.Add(
                        new DetailsBySlotid()
                        {
                            Slot_id = Convert.ToInt32(dt.Rows[i]["slot_id"]),
                            Meeting_schedule_id = Convert.ToInt32(dt.Rows[i]["eb_meeting_schedule_id"]),
                            MeetingId = Convert.ToInt32(dt.Rows[i]["eb_meeting_id"]),
                            No_Attendee = Convert.ToInt32(dt.Rows[i]["no_of_attendee"]),
                            No_Host = Convert.ToInt32(dt.Rows[i]["no_of_host"]),
                            SlotHostCount = Convert.ToInt32(dt.Rows[i]["slot_host_count"]),
                            SlotAttendeeCount = Convert.ToInt32(dt.Rows[i]["slot_attendee_count"]),
                            Is_approved = Convert.ToString(dt.Rows[i]["is_approved"]),
                            Participant_id = Convert.ToInt32(dt.Rows[i]["participant_id"]),
                        });
                }
                if (request.SlotParticipant.Participant_type == 2)

                {
                    if (SlotObj[0].No_Attendee >= SlotObj[0].SlotAttendeeCount)
                    {
                        Status = true;
                    }
                }
                else
                {
                    if (SlotObj[0].No_Host >= SlotObj[0].SlotHostCount)
                    {
                        Status = true;
                    }
                }
            }
            catch (Exception e)
            {
                Status = false;
                Console.WriteLine(e.Message, e.StackTrace);
            }

            if (Status)
            {
                if (SlotObj[0].Is_approved == "T")
                {
                    query = $"insert into eb_meeting_slot_participants( user_id ,role_id ,user_group_id , confirmation , eb_meeting_schedule_id , approved_slot_id ,name ,email,phone_num," +
                            $" type_of_user,participant_type) values ({request.SlotParticipant.UserId},{request.SlotParticipant.RoleId},{request.SlotParticipant.UserGroupId},{request.SlotParticipant.Confirmation}," +
                            $"{SlotObj[0].Meeting_schedule_id},{request.SlotParticipant.ApprovedSlotId},'{request.SlotParticipant.Name}','{request.SlotParticipant.Email}','{request.SlotParticipant.PhoneNum}'," +
                            $"{request.SlotParticipant.TypeOfUser},{request.SlotParticipant.Participant_type});" +
                            $"insert into eb_meeting_participants (eb_meeting_id, eb_slot_participant_id ) values ({SlotObj[0].MeetingId} ,eb_currval('eb_meeting_slot_participants_id_seq'));";
                }
                else if (SlotObj[0].Is_approved == "F")
                {
                    query = $"insert into eb_meetings (eb_meeting_slots_id , eb_created_by)values({SlotObj[0].Slot_id}, 1);" +
                            $"insert into eb_meeting_slot_participants( user_id ,role_id ,user_group_id , confirmation , eb_meeting_schedule_id , approved_slot_id ,name ,email,phone_num," +
                            $" type_of_user,participant_type) values ({request.SlotParticipant.UserId},{request.SlotParticipant.RoleId},{request.SlotParticipant.UserGroupId},{request.SlotParticipant.Confirmation}," +
                            $"{SlotObj[0].Meeting_schedule_id},{request.SlotParticipant.ApprovedSlotId},'{request.SlotParticipant.Name}','{request.SlotParticipant.Email}','{request.SlotParticipant.PhoneNum}'," +
                            $"{request.SlotParticipant.TypeOfUser},{request.SlotParticipant.Participant_type});";
                    for (int i = 0; i < SlotObj.Count(); i++)
                    {
                        query += $"insert into eb_meeting_participants (eb_meeting_id, eb_slot_participant_id ) values ( eb_currval('eb_meetings_id_seq'),{SlotObj[i].Participant_id} );";
                    }
                    query += $"insert into eb_meeting_participants (eb_meeting_id, eb_slot_participant_id ) values (eb_currval('eb_meetings_id_seq') , eb_currval('eb_meeting_slot_participants_id_seq'));";
                    query += $"update eb_meeting_slots set is_approved = 'T' where  id ={request.SlotParticipant.ApprovedSlotId}";
                }

                try
                {
                    EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query);
                    Resp.ResponseStatus = true;
                }
                catch (Exception e)
                {
                    Resp.ResponseStatus = false;
                    Console.WriteLine(e.Message, e.StackTrace);
                }
            }
            return Resp;
        }
        public AddMeetingSlotResponse Post(AddMeetingSlotRequest request)
        {
            string qry = "";
            string date = request.Date;
            TimeSpan today = new TimeSpan(09, 00, 00);
            TimeSpan duration = new System.TimeSpan(00, 29, 00);
            TimeSpan intervals = new System.TimeSpan(00, 30, 00);
            for (int i = 0; i < 14; i++)
            {
                TimeSpan temp = today.Add(duration);
                qry += $"insert into eb_meeting_slots (eb_meeting_schedule_id,meeting_date,time_from,time_to,eb_created_by) values " +
                    $"('1','{request.Date}','{today}','{temp}', 2 );";
                today = today.Add(intervals);
            }
            try
            {
                int a = this.EbConnectionFactory.DataDB.DoNonQuery(qry);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace, e.Message);
            }
            return new AddMeetingSlotResponse();
        }

        public GetMeetingsDetailsResponse Post(GetMeetingsDetailsRequest request)
        {
            GetMeetingsDetailsResponse Resp = new GetMeetingsDetailsResponse();
            string _qry1 = $@"
            SELECT id,COALESCE (eb_meeting_schedule_id, 0)as meeting_schedule_id,COALESCE (eb_meeting_slots_id, 0)as eb_meeting_slots_id , form_ref_id FROM  eb_my_actions 
	                     WHERE  eb_del = 'F' and id ={request.MyActionId} and is_completed ='F';";
            EbDataSet SdTest = this.EbConnectionFactory.DataDB.DoQueries(_qry1);
            
            string qry_ = "";
            try
            {
                Resp.FormRefid = Convert.ToString(SdTest.Tables[0].Rows[0]["form_ref_id"]);
                int MeetingSlotId = Convert.ToInt32(SdTest.Tables[0].Rows[0]["eb_meeting_slots_id"]);
                int MeetingScheduleId = Convert.ToInt32(SdTest.Tables[0].Rows[0]["meeting_schedule_id"]);
                if (MeetingSlotId != 0)
                {
                    qry_ += $@"
                  SELECT B.id,B.title,B.description,B.meeting_date,B.time_from,B.time_to,B.venue,B.integration FROM
				   (SELECT id, eb_meeting_schedule_id FROM eb_meeting_slots
							 where id = {MeetingSlotId} )A
							 LEFT JOIN
							 (SELECT id,title,description, meeting_date,time_from,time_to,venue,integration 
							 from eb_meeting_schedule )B
							 on B.id = A.eb_meeting_schedule_id;
                select id,meeting_date,time_from,time_to,eb_meeting_schedule_id,is_approved from eb_meeting_slots where id = {MeetingSlotId} and eb_del='F';
                SELECT A.id,A.approved_slot_id,A.eb_meeting_schedule_id,A.user_id,A.type_of_user,A.participant_type,A.confirmation,
                 B.fullname,B.email FROM
				   (SELECT id , approved_slot_id, eb_meeting_schedule_id , user_id ,type_of_user,participant_type,confirmation FROM  eb_meeting_slot_participants
							 where approved_slot_id = {MeetingSlotId} and participant_type = 1)A
							 LEFT JOIN	
							 (select id, fullname ,email from eb_users where eb_del = 'F')B
							 ON B.id = A.user_id;
                 SELECT A.id,A.approved_slot_id,A.eb_meeting_schedule_id,A.user_id,A.type_of_user,A.participant_type,A.confirmation,
                 B.name,B.email,B.form_refid,B.form_data_id
			                        FROM
				                   (SELECT id , approved_slot_id, eb_meeting_schedule_id , user_id ,type_of_user,participant_type,confirmation FROM  
                                    eb_meeting_slot_participants where approved_slot_id = {MeetingSlotId} and participant_type = 2)A
							                 LEFT JOIN	
							                 (select id, name ,email,form_refid,form_data_id from eb_contacts where eb_del = 'F')B
							                 ON B.id = A.user_id;
                                ";
                    EbDataSet ds = this.EbConnectionFactory.DataDB.DoQueries(qry_);
                    Resp.MeetingScheduleDetails.MeetingScheduleId = Convert.ToInt32(ds.Tables[0].Rows[0]["id"]);
                    Resp.MeetingScheduleDetails.Title = Convert.ToString(ds.Tables[0].Rows[0]["title"]);
                    Resp.MeetingScheduleDetails.Description = Convert.ToString(ds.Tables[0].Rows[0]["description"]);
                    Resp.MeetingScheduleDetails.Date = Convert.ToString(ds.Tables[0].Rows[0]["meeting_date"]);
                    Resp.MeetingScheduleDetails.TimeFrom = Convert.ToString(ds.Tables[0].Rows[0]["time_from"]);
                    Resp.MeetingScheduleDetails.TimeTo = Convert.ToString(ds.Tables[0].Rows[0]["time_to"]);
                    Resp.MeetingScheduleDetails.Location = Convert.ToString(ds.Tables[0].Rows[0]["venue"]);
                    Resp.MeetingScheduleDetails.Integration = Convert.ToString(ds.Tables[0].Rows[0]["integration"]);
                    for (int i = 0; i < ds.Tables[1].Rows.Count; i++)
                    {
                        Resp.SlotList.Add(
                            new SlotTable()
                            {
                                SlotId = Convert.ToInt32(ds.Tables[1].Rows[i]["id"]),
                                Date = Convert.ToString(ds.Tables[1].Rows[i]["meeting_date"]),
                                TimeFrom = Convert.ToString(ds.Tables[1].Rows[i]["time_from"]),
                                TimeTo = Convert.ToString(ds.Tables[1].Rows[i]["time_to"]),
                                MeetingScheduleId = Convert.ToInt32(ds.Tables[1].Rows[i]["eb_meeting_schedule_id"]),
                                IsApproved = Convert.ToString(ds.Tables[1].Rows[i]["is_approved"]),
                            });
                    }
                    for (int i = 0; i < ds.Tables[2].Rows.Count; i++)
                    {
                        Resp.ParticipantList.Add(
                            new MeetingParticipants()
                            {
                                MeetingScheduleId = Convert.ToInt32(ds.Tables[2].Rows[i]["eb_meeting_schedule_id"]),
                                ApprovedSlotId = Convert.ToInt32(ds.Tables[2].Rows[i]["approved_slot_id"]),
                                Id = Convert.ToInt32(ds.Tables[2].Rows[i]["id"]),
                                UserId = Convert.ToInt32(ds.Tables[2].Rows[i]["user_id"]),
                                TypeOfUser = Convert.ToInt32(ds.Tables[2].Rows[i]["type_of_user"]),
                                ParticipantType = Convert.ToInt32(ds.Tables[2].Rows[i]["participant_type"]),
                                Confirmation = Convert.ToInt32(ds.Tables[2].Rows[i]["confirmation"]),
                                Name = Convert.ToString(ds.Tables[2].Rows[i]["fullname"]),
                                Email = Convert.ToString(ds.Tables[2].Rows[i]["email"]),

                            });
                    }
                    for (int i = 0; i < ds.Tables[3].Rows.Count; i++)
                    {
                        Resp.ParticipantList.Add(
                            new MeetingParticipants()
                            {
                                MeetingScheduleId = Convert.ToInt32(ds.Tables[3].Rows[i]["eb_meeting_schedule_id"]),
                                ApprovedSlotId = Convert.ToInt32(ds.Tables[3].Rows[i]["approved_slot_id"]),
                                Id = Convert.ToInt32(ds.Tables[3].Rows[i]["id"]),
                                UserId = Convert.ToInt32(ds.Tables[3].Rows[i]["user_id"]),
                                TypeOfUser = Convert.ToInt32(ds.Tables[3].Rows[i]["type_of_user"]),
                                ParticipantType = Convert.ToInt32(ds.Tables[3].Rows[i]["participant_type"]),
                                Confirmation = Convert.ToInt32(ds.Tables[1].Rows[i]["confirmation"]),
                                Name = Convert.ToString(ds.Tables[3].Rows[i]["name"]),
                                Email = Convert.ToString(ds.Tables[3].Rows[i]["email"]),
                                FormDataId = Convert.ToInt32(ds.Tables[3].Rows[i]["form_data_id"]),
                                FormRefid = Convert.ToString(ds.Tables[3].Rows[i]["form_refid"]),
                            });
                    }
                    Resp.IsDirectMeeting = true;
                }
                else if (MeetingScheduleId != 0)
                {
                    qry_ += $@"
            SELECT id,title,description, meeting_date,time_from,time_to,venue,integration from eb_meeting_schedule where id ={MeetingScheduleId} and eb_del ='F';
            select id,meeting_date,time_from,time_to,eb_meeting_schedule_id,is_approved from eb_meeting_slots where eb_meeting_schedule_id = {MeetingScheduleId} and eb_del='F'

            ";
                    EbDataSet ds = this.EbConnectionFactory.DataDB.DoQueries(qry_);
                    Resp.MeetingScheduleDetails.MeetingScheduleId = Convert.ToInt32(ds.Tables[0].Rows[0]["id"]);
                    Resp.MeetingScheduleDetails.Title = Convert.ToString(ds.Tables[0].Rows[0]["title"]);
                    Resp.MeetingScheduleDetails.Description = Convert.ToString(ds.Tables[0].Rows[0]["description"]);
                    Resp.MeetingScheduleDetails.Date = Convert.ToString(ds.Tables[0].Rows[0]["meeting_date"]);
                    Resp.MeetingScheduleDetails.TimeFrom = Convert.ToString(ds.Tables[0].Rows[0]["time_from"]);
                    Resp.MeetingScheduleDetails.TimeTo = Convert.ToString(ds.Tables[0].Rows[0]["time_to"]);
                    Resp.MeetingScheduleDetails.Location = Convert.ToString(ds.Tables[0].Rows[0]["venue"]);
                    Resp.MeetingScheduleDetails.Integration = Convert.ToString(ds.Tables[0].Rows[0]["integration"]);
                    for (int i = 0; i < ds.Tables[1].Rows.Count; i++)
                    {
                        Resp.SlotList.Add(
                            new SlotTable()
                            {
                                SlotId = Convert.ToInt32(ds.Tables[1].Rows[i]["id"]),
                                Date = Convert.ToString(ds.Tables[1].Rows[i]["meeting_date"]),
                                TimeFrom = Convert.ToString(ds.Tables[1].Rows[i]["time_from"]),
                                TimeTo = Convert.ToString(ds.Tables[1].Rows[i]["time_to"]),
                                MeetingScheduleId = Convert.ToInt32(ds.Tables[1].Rows[i]["eb_meeting_schedule_id"]),
                                IsApproved = Convert.ToString(ds.Tables[1].Rows[i]["is_approved"]),
                            });
                    }
                    Resp.IsDirectMeeting = false;
                }
                Resp.ResponseStatus = true;
            }
            //            string _qry = $@"
            //            SELECT id,COALESCE (eb_meeting_schedule_id, 0)as meeting_schedule_id, COALESCE (eb_meeting_slots_id, 0)as eb_meeting_slots_id FROM  eb_my_actions 
            //	                     WHERE  eb_del = 'F' and id ={request.MyActionId} and is_completed ='F';
            //                 SELECT 
            //	            A.id,A.is_completed, B.id as slot_id,C.id as meeting_schedule_id,C.description,B.time_from,B.time_to,
            //				C.meeting_date ,C.venue,C.integration,C.title,
            //				D.user_id , D.type_of_user,D.participant_type,E.fullname
            //			        FROM
            //				   (SELECT
            //						id, eb_meeting_slots_id,is_completed FROM  eb_my_actions 
            //	                     WHERE  eb_del = 'F' and id ={request.MyActionId} )A
            //						LEFT JOIN
            //							 (SELECT id , eb_meeting_schedule_id,time_from,time_to FROM  eb_meeting_slots)B
            //							 ON B.id = A.eb_meeting_slots_id		
            //							 LEFT JOIN	
            //							 (SELECT id ,title, meeting_date,venue,integration,description FROM  eb_meeting_schedule )C
            //							 ON C.id = B.eb_meeting_schedule_id	
            //							 LEFT JOIN	
            //							 (SELECT id , approved_slot_id, eb_meeting_schedule_id , user_id ,type_of_user,participant_type FROM  eb_meeting_slot_participants )D
            //							 ON D.approved_slot_id = B.id and participant_type = 1
            //							 LEFT JOIN	
            //							 (select id, fullname from eb_users where eb_del = 'F')E
            //							 ON E.id = D.user_id;

            //                SELECT 
            //	            A.id,A.eb_meeting_schedule_id,B.id as slot_id,B.time_from,B.time_to,B.is_approved,
            //				C.title,C.description,C.meeting_date,C.venue,C.integration
            //			        FROM
            //				   (SELECT id ,eb_meeting_schedule_id,eb_meeting_slots_id FROM  eb_my_actions 
            //	                     WHERE  eb_del = 'F' and id ={request.MyActionId} and is_completed ='F' )A
            //						LEFT JOIN
            //							 (select eb_meeting_schedule_id,id,meeting_date,time_from,time_to,is_approved 
            //							  from eb_meeting_slots  where eb_del ='F')B
            //							 ON B.eb_meeting_schedule_id = A.eb_meeting_schedule_id		
            //							 LEFT JOIN
            //							 (select id,title,description,meeting_date,venue,integration from eb_meeting_schedule 
            //							  where  eb_del ='F')C
            //							  ON C.id = A.eb_meeting_schedule_id
            //							 where A.eb_meeting_schedule_id is not null
            //";

            //            try
            //            {
            //                EbDataSet ds = this.EbConnectionFactory.DataDB.DoQueries(_qry);

            //                Resp.MyActionDetails.Id = Convert.ToInt32(ds.Tables[0].Rows[0]["id"]);
            //                Resp.MyActionDetails.MeetingScheduleId = Convert.ToInt32(ds.Tables[0].Rows[0]["meeting_schedule_id"]);
            //                Resp.MyActionDetails.SlotId = Convert.ToInt32(ds.Tables[0].Rows[0]["eb_meeting_slots_id"]);
            //                for (int i = 0; i < ds.Tables[1].Rows.Count; i++)
            //                {
            //                    Resp.MeetingRequest.Add(
            //                        new MeetingRequest()
            //                        {
            //                            MaId = Convert.ToInt32(ds.Tables[1].Rows[i]["id"]),
            //                            Slotid = Convert.ToInt32(ds.Tables[1].Rows[i]["slot_id"]),
            //                            MaIsCompleted = Convert.ToString(ds.Tables[1].Rows[i]["is_completed"]),
            //                            MeetingScheduleid = Convert.ToInt32(ds.Tables[1].Rows[i]["meeting_schedule_id"]),
            //                            Description = Convert.ToString(ds.Tables[1].Rows[i]["description"]),
            //                            TimeFrom = Convert.ToString(ds.Tables[1].Rows[i]["time_from"]),
            //                            TimeTo = Convert.ToString(ds.Tables[1].Rows[i]["time_to"]),
            //                            Title = Convert.ToString(ds.Tables[1].Rows[i]["title"]),
            //                            MeetingDate = Convert.ToString(ds.Tables[1].Rows[i]["meeting_date"]),
            //                            Venue = Convert.ToString(ds.Tables[1].Rows[i]["venue"]),
            //                            Integration = Convert.ToString(ds.Tables[1].Rows[i]["integration"]),
            //                            fullname = Convert.ToString(ds.Tables[1].Rows[i]["fullname"]),
            //                            UserId = Convert.ToInt32(ds.Tables[1].Rows[i]["user_id"]),
            //                            TypeofUser = Convert.ToInt32(ds.Tables[1].Rows[i]["type_of_user"]),
            //                            ParticipantType = Convert.ToInt32(ds.Tables[1].Rows[i]["participant_type"]),
            //                        });
            //                }
            //                for (int i = 0; i < ds.Tables[2].Rows.Count; i++)
            //                {
            //                    Resp.SlotsRequest.Add(
            //                        new SlotsRequest()
            //                        {
            //                            MyActionId = Convert.ToInt32(ds.Tables[2].Rows[i]["id"]),
            //                            MeetingScheduleId = Convert.ToInt32(ds.Tables[2].Rows[i]["eb_meeting_schedule_id"]),
            //                            SlotId = Convert.ToInt32(ds.Tables[2].Rows[i]["slot_id"]),
            //                            TimeFrom = Convert.ToString(ds.Tables[2].Rows[i]["time_from"]),
            //                            TimeTo = Convert.ToString(ds.Tables[2].Rows[i]["time_to"]),
            //                            IsApproved = Convert.ToString(ds.Tables[2].Rows[i]["is_approved"]),
            //                            Title = Convert.ToString(ds.Tables[2].Rows[i]["title"]),
            //                            Description = Convert.ToString(ds.Tables[2].Rows[i]["description"]),
            //                            Venue = Convert.ToString(ds.Tables[2].Rows[i]["venue"]),
            //                            Integration = Convert.ToString(ds.Tables[2].Rows[i]["integration"]),
            //                            MeetingDate = Convert.ToString(ds.Tables[2].Rows[i]["meeting_date"]),
            //                        });
            //                }

            //            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace, e.Message);
                Resp.ResponseStatus = false;
            }
            return Resp;
        }

        public GetScheduleUserDetailsResponse Post(GetScheduleUserDetailsRequest request)
        {
            GetScheduleUserDetailsResponse obj = new GetScheduleUserDetailsResponse();
            string _qry = $@"select participant_type from (SELECT id,COALESCE (eb_meeting_schedule_id, 0)as meeting_schedule_id,COALESCE (eb_meeting_slots_id, 0)as eb_meeting_slots_id , form_ref_id,
			   COALESCE(user_ids, '')as user_ids,COALESCE(role_ids, '')as role_ids,COALESCE(usergroup_id, 0)as user_group_id
			   FROM  eb_my_actions 
	                     WHERE  eb_del = 'F' and id ={request.MyActionId} and is_completed ='F' ) A 
	left join (select COALESCE (eb_meeting_schedule_id, 0)as meeting_schedule_id,participant_type,
			   COALESCE(user_ids, '')as user_ids,COALESCE(role_ids, '')as role_ids,COALESCE(user_group_id, 0)as user_group_id 
			   from eb_meeting_scheduled_participants) B on 
	B.meeting_schedule_id = A.meeting_schedule_id and B.user_ids = A.user_ids and B.role_ids = A.role_ids and B.user_group_id = A.user_group_id";
            EbDataSet SdTest = this.EbConnectionFactory.DataDB.DoQueries(_qry);
            try
            {
                if(Convert.ToInt32(SdTest.Tables[0].Rows[0]["participant_type"]) == 1)
                {
                    obj.ParticipantType = ParticipantType.Host;
                }
            }
            catch(Exception e)
            {
                Console.WriteLine(e.StackTrace, e.Message);
            }
            return obj;
        }

        public MeetingUpdateByUsersResponse Post(MeetingUpdateByUsersRequest request)
        {
            MeetingSaveValidateResponse Resp = new MeetingSaveValidateResponse();
            string query = @"        
            SELECT 
		     A.id as slot_id , A.eb_meeting_schedule_id, A.is_approved,
			 B.no_of_attendee, B.no_of_hosts,B.max_hosts,B.max_attendees,
			 COALESCE (D.id, 0) as meeting_id
	            FROM
				(SELECT 
						id, eb_meeting_schedule_id , is_approved, 
					meeting_date, time_from, time_to
	                     FROM 
		                     eb_meeting_slots 
	                     WHERE 
		                     eb_del = 'F' and id = {0})A
						LEFT JOIN	 
							 (SELECT id, no_of_attendee, no_of_hosts,max_hosts,max_attendees  FROM  eb_meeting_schedule)B
							 ON
 	                     B.id = A.eb_meeting_schedule_id	
                     LEFT JOIN 
                     (SELECT 
		                     id, eb_meeting_slots_id
	                     FROM 
		                     eb_meetings
	                     where
		                     eb_del = 'F') D
		                     ON
 	                     D.eb_meeting_slots_id = A.id ;
                SELECT 
		     A.id as slot_id , A.eb_meeting_schedule_id,
			 COALESCE (B.id, 0) as participant_id,B.participant_type,B.type_of_user,B.user_id,B.confirmation
	            FROM
				(SELECT id, eb_meeting_schedule_id
	                     FROM  eb_meeting_slots 
	                     WHERE  eb_del = 'F' and id = {0})A
						LEFT JOIN	
						(SELECT id, user_id,eb_meeting_schedule_id,approved_slot_id ,type_of_user,participant_type,confirmation
	                     FROM eb_meeting_slot_participants
	                     GROUP BY
		                     id,user_id,eb_meeting_schedule_id, approved_slot_id, type_of_user,participant_type, eb_del,confirmation
	                     Having eb_del = 'F')B
                     ON B.eb_meeting_schedule_id = A.eb_meeting_schedule_id and B.approved_slot_id = A.id 
                         where participant_type is not null; 
 
 						select count(*) as slot_attendee_count from eb_meeting_slot_participants where approved_slot_id = {0} 
									   and participant_type=2 and confirmation = 1;
						select count(*) as slot_host_count from eb_meeting_slot_participants where approved_slot_id = {0} 
									   and participant_type=1 and confirmation = 1;
            select id, user_ids,usergroup_id,role_ids, form_ref_id, form_data_id , description, expiry_datetime, eb_meeting_slots_id ,except_user_ids from eb_my_actions 
            where eb_meeting_slots_id = {0} and id= {1} and is_completed='F';
                select id, meeting_opts  from eb_meeting_slots where id = {0};
                                        ";

            List<MyAction> MyActionObj = new List<MyAction>();

            List<MeetingScheduleDetails> MSD = new List<MeetingScheduleDetails>(); //MSD Meeting Schedule Details
            List<SlotParticipantsDetails> SPL = new List<SlotParticipantsDetails>(); //SPL Slot Participant List
            SlotParticipantsDetails CurrentUser = new SlotParticipantsDetails(); //SPL Slot Participant List
            SlotParticipantCount SPC = new SlotParticipantCount(); //SPL Slot Participant Count
            MeetingOptions Type = MeetingOptions.F_H_F_A;

            MeetingUpdateByUsersResponse resp = new MeetingUpdateByUsersResponse();
            resp.ResponseStatus = true;
            bool Status = false;
            //bool IsDirectMeeting = false;
            try
            {
                String _query = string.Format(query, request.Id, request.MyActionId); ;
                EbDataSet ds = this.EbConnectionFactory.DataDB.DoQueries(_query);
                for (int k = 0; k < ds.Tables[0].Rows.Count; k++)
                {
                    MSD.Add(new MeetingScheduleDetails()
                    {
                        SlotId = Convert.ToInt32(ds.Tables[0].Rows[k]["slot_id"]),
                        MeetingScheduleId = Convert.ToInt32(ds.Tables[0].Rows[k]["eb_meeting_schedule_id"]),
                        MeetingId = Convert.ToInt32(ds.Tables[0].Rows[k]["meeting_id"]),
                        MinAttendees = Convert.ToInt32(ds.Tables[0].Rows[k]["no_of_attendee"]),
                        MinHosts = Convert.ToInt32(ds.Tables[0].Rows[k]["no_of_hosts"]),
                        MaxAttendees = Convert.ToInt32(ds.Tables[0].Rows[k]["max_attendees"]),
                        MaxHosts = Convert.ToInt32(ds.Tables[0].Rows[k]["max_hosts"]),
                        IsApproved = Convert.ToString(ds.Tables[0].Rows[k]["is_approved"]),
                        //MeetingOpts = (MeetingOptions)Convert.ToInt32(ds.Tables[0].Rows[k]["meeting_opts"])
                    });
                }
                for (int k = 0; k < ds.Tables[1].Rows.Count; k++)
                {
                    SPL.Add(new SlotParticipantsDetails()
                    {
                        SlotId = Convert.ToInt32(ds.Tables[1].Rows[k]["slot_id"]),
                        MeetingScheduleId = Convert.ToInt32(ds.Tables[1].Rows[k]["eb_meeting_schedule_id"]),
                        ParticipantId = Convert.ToInt32(ds.Tables[1].Rows[k]["participant_id"]),
                        ParticipantType = Convert.ToInt32(ds.Tables[1].Rows[k]["participant_type"]),
                        TypeOfUser = Convert.ToInt32(ds.Tables[1].Rows[k]["type_of_user"]),
                        UserId = Convert.ToInt32(ds.Tables[1].Rows[k]["user_id"]),
                        Confirmation = Convert.ToInt32(ds.Tables[1].Rows[k]["confirmation"]),
                    });
                }
                SPC.SlotAttendeeCount = Convert.ToInt32(ds.Tables[2].Rows[0]["slot_attendee_count"]);
                SPC.SlotHostCount = Convert.ToInt32(ds.Tables[3].Rows[0]["slot_host_count"]);
                for (int i = 0; i < ds.Tables[4].Rows.Count; i++)
                {
                    MyActionObj.Add(new MyAction()
                    {
                        Id = Convert.ToInt32(ds.Tables[4].Rows[i]["id"]),
                        SlotId = Convert.ToInt32(ds.Tables[4].Rows[i]["eb_meeting_slots_id"]),
                        Description = Convert.ToString(ds.Tables[4].Rows[i]["description"]),
                        UserIds = Convert.ToString(ds.Tables[4].Rows[i]["user_ids"]),
                        RoleIds = Convert.ToString(ds.Tables[4].Rows[i]["role_ids"]),
                        FormRefId = Convert.ToString(ds.Tables[4].Rows[i]["form_ref_id"]),
                        ExpiryDateTime = Convert.ToString(ds.Tables[4].Rows[i]["expiry_datetime"]),
                        ExceptUserIds = Convert.ToString(ds.Tables[4].Rows[i]["except_user_ids"]),
                        UserGroupId = Convert.ToInt32(ds.Tables[4].Rows[i]["usergroup_id"]),
                        FormDataId = Convert.ToInt32(ds.Tables[4].Rows[i]["form_data_id"]),

                    });
                }
                Type = (MeetingOptions)Convert.ToInt32(ds.Tables[5].Rows[0]["meeting_opts"]);
            }
            catch (Exception e)
            {
                Status = false;
                Console.WriteLine(e.Message, e.StackTrace);
            }
            string qry_ = "";
            for (int i = 0; i < SPL.Count; i++)
            {
                if (SPL[i].UserId == request.UserInfo.UserId)
                {
                    CurrentUser.UserId = SPL[i].UserId;
                    CurrentUser.TypeOfUser = SPL[i].TypeOfUser;
                    CurrentUser.ParticipantId = SPL[i].ParticipantId;
                    CurrentUser.ParticipantType = SPL[i].ParticipantType;
                    CurrentUser.ParticipantType = SPL[i].ParticipantType;
                    CurrentUser.SlotId = SPL[i].SlotId;
                    CurrentUser.MeetingScheduleId = SPL[i].MeetingScheduleId;
                    // IsDirectMeeting = true;
                }
            }
            if (Type == MeetingOptions.F_H_F_A)
                qry_ += F_H_F_A(MyActionObj, MSD, SPL, CurrentUser, SPC, request);
            else if (Type == MeetingOptions.F_H_E_A)
                qry_ += F_H_E_A(MyActionObj, MSD, SPL, CurrentUser, SPC, request);
            else if (Type == MeetingOptions.E_H_F_A)
                qry_ += E_H_F_A(MyActionObj, MSD, SPL, CurrentUser, SPC, request);
            try
            {
                int a = this.EbConnectionFactory.DataDB.DoNonQuery(qry_);
            }
            catch (Exception e)
            {
                resp.ResponseStatus = false;
                Console.WriteLine(e.StackTrace, e.Message);
            }
            return resp;
        }

        public string F_H_F_A(List<MyAction> MyActionObj, List<MeetingScheduleDetails> MSD, List<SlotParticipantsDetails> SPL, SlotParticipantsDetails CurrentUser, SlotParticipantCount SPC, MeetingUpdateByUsersRequest request)
        {
            string qry_ = "";
            if (CurrentUser.ParticipantType == 1 && MyActionObj.Count != 0 && MSD[0].MaxHosts > SPC.SlotHostCount)
            {
                if (MSD[0].IsApproved == "F" && MSD[0].MinHosts == (SPC.SlotHostCount + 1) && MSD[0].MinAttendees <= SPC.SlotAttendeeCount && MSD[0].MaxAttendees >= SPC.SlotAttendeeCount)
                {
                    qry_ += Insert_EbMeetings(MSD[0].SlotId, request.UserInfo.UserId);
                    qry_ += $@"update eb_meeting_slot_participants set confirmation = 1 where id = {CurrentUser.ParticipantId} ; ";
                    for (int k = 0; k < SPL.Count; k++)
                    {
                        if (SPL[k].Confirmation == 1)
                            qry_ += Insert_Participants(0, SPL[k].ParticipantId);
                    }
                    qry_ += $"update eb_meeting_slots set is_approved = 'T' where  id = {request.Id}; ";
                }
                else if (MSD[0].IsApproved == "T" && MSD[0].MeetingId > 0)
                {
                    qry_ += $@"update eb_meeting_slot_participants set confirmation = 1 where id = {CurrentUser.ParticipantId} ;  ";
                    qry_ += Insert_Participants(MSD[0].MeetingId, CurrentUser.ParticipantId);
                }
                else if (MSD[0].IsApproved == "F")
                {
                    qry_ += $@"update eb_meeting_slot_participants set confirmation = 1 where id = {CurrentUser.ParticipantId} ; ";
                }

                if (CurrentUser.ParticipantType == 1 && MSD[0].MaxHosts == (SPC.SlotHostCount + 1))
                {
                    qry_ += Insert_Myaction(MyActionObj, request.Id, request.UserInfo.UserId, request.MyActionId, 2);
                }
                else
                {
                    qry_ += Insert_Myaction(MyActionObj, request.Id, request.UserInfo.UserId, request.MyActionId, 2);
                    qry_ += Insert_Myaction(MyActionObj, request.Id, request.UserInfo.UserId, request.MyActionId, 1);
                }

            }
            else if (CurrentUser.ParticipantType == 2 && MyActionObj.Count != 0 && MSD[0].MaxAttendees > SPC.SlotAttendeeCount)
            {
                if (MSD[0].IsApproved == "F" && MSD[0].MinAttendees == (SPC.SlotAttendeeCount + 1) && MSD[0].MinHosts <= SPC.SlotHostCount && MSD[0].MaxHosts >= SPC.SlotHostCount)
                {
                    qry_ += Insert_EbMeetings(MSD[0].SlotId, request.UserInfo.UserId);
                    qry_ += $@"update eb_meeting_slot_participants set confirmation = 1 where id = {CurrentUser.ParticipantId} ; ";
                    for (int k = 0; k < SPL.Count; k++)
                    {
                        if (SPL[k].Confirmation == 1)
                            qry_ += Insert_Participants(0, SPL[k].ParticipantId);
                    }
                    qry_ += $"update eb_meeting_slots set is_approved = 'T' where  id = {request.Id}; ";
                }
                else if (MSD[0].IsApproved == "T" && MSD[0].MeetingId > 0)
                {
                    qry_ += $@"update eb_meeting_slot_participants set confirmation = 1 where id = {CurrentUser.ParticipantId} ;";
                    qry_ += Insert_Participants(MSD[0].MeetingId, CurrentUser.ParticipantId);
                }
                else if (MSD[0].IsApproved == "F")
                {
                    qry_ += $@"update eb_meeting_slot_participants set confirmation = 1 where id = {CurrentUser.ParticipantId} ; ";
                }

                if (CurrentUser.ParticipantType == 2 && MSD[0].MaxAttendees == (SPC.SlotAttendeeCount + 1))
                {
                    qry_ += Insert_Myaction(MyActionObj, request.Id, request.UserInfo.UserId, request.MyActionId, 2);
                }
                else
                {
                    qry_ += Insert_Myaction(MyActionObj, request.Id, request.UserInfo.UserId, request.MyActionId, 2);
                    qry_ += Insert_Myaction(MyActionObj, request.Id, request.UserInfo.UserId, request.MyActionId, 1);
                }
            }
            return qry_;
        }

        //Fixed Host Eligible Attendee meeting Update
        public string F_H_E_A(List<MyAction> MyActionObj, List<MeetingScheduleDetails> MSD, List<SlotParticipantsDetails> SPL, SlotParticipantsDetails CurrentUser, SlotParticipantCount SPC, MeetingUpdateByUsersRequest request)
        {
            string qry_ = "";
            if (CurrentUser.ParticipantType == 1 && MyActionObj.Count != 0 && MSD[0].MaxHosts > SPC.SlotHostCount)
            {
                if (MSD[0].IsApproved == "F" && MSD[0].MinHosts == (SPC.SlotHostCount + 1) && MSD[0].MinAttendees <= SPC.SlotAttendeeCount && MSD[0].MaxAttendees >= SPC.SlotAttendeeCount)
                {
                    qry_ += Insert_EbMeetings(MSD[0].SlotId, request.UserInfo.UserId);
                    qry_ += $@"update eb_meeting_slot_participants set confirmation = 1 where id = {CurrentUser.ParticipantId} ; ";
                    for (int k = 0; k < SPL.Count; k++)
                    {
                        if (SPL[k].Confirmation == 1)
                            qry_ += Insert_Participants(0, SPL[k].ParticipantId);
                    }
                    qry_ += $"update eb_meeting_slots set is_approved = 'T' where  id = {request.Id}; ";
                }
                else if (MSD[0].IsApproved == "T" && MSD[0].MeetingId > 0)
                {
                    qry_ += $@"update eb_meeting_slot_participants set confirmation = 1 where id = {CurrentUser.ParticipantId} ;  ";
                    qry_ += Insert_Participants(MSD[0].MeetingId, CurrentUser.ParticipantId);
                }
                else if (MSD[0].IsApproved == "F")
                {
                    qry_ += $@"update eb_meeting_slot_participants set confirmation = 1 where id = {CurrentUser.ParticipantId} ; ";
                }

                if (CurrentUser.ParticipantType == 1 && MSD[0].MaxHosts == (SPC.SlotHostCount + 1))
                {
                    qry_ += Insert_Myaction(MyActionObj, request.Id, request.UserInfo.UserId, request.MyActionId, 2);
                }
                else
                {
                    qry_ += Insert_Myaction(MyActionObj, request.Id, request.UserInfo.UserId, request.MyActionId, 2);
                    qry_ += Insert_Myaction(MyActionObj, request.Id, request.UserInfo.UserId, request.MyActionId, 1);
                }

            }

            return qry_;
        }
        public string E_H_F_A(List<MyAction> MyActionObj, List<MeetingScheduleDetails> MSD, List<SlotParticipantsDetails> SPL, SlotParticipantsDetails CurrentUser, SlotParticipantCount SPC, MeetingUpdateByUsersRequest request)
        {
            string qry_ = "";
            if (CurrentUser.ParticipantType == 2 && MyActionObj.Count != 0 && MSD[0].MaxAttendees > SPC.SlotAttendeeCount)
            {
                if (MSD[0].IsApproved == "F" && MSD[0].MinAttendees == (SPC.SlotAttendeeCount + 1) && MSD[0].MinHosts <= SPC.SlotHostCount && MSD[0].MaxHosts >= SPC.SlotHostCount)
                {
                    qry_ += Insert_EbMeetings(MSD[0].SlotId, request.UserInfo.UserId);
                    qry_ += $@"update eb_meeting_slot_participants set confirmation = 1 where id = {CurrentUser.ParticipantId} ; ";
                    for (int k = 0; k < SPL.Count; k++)
                    {
                        if (SPL[k].Confirmation == 1)
                            qry_ += Insert_Participants(0, SPL[k].ParticipantId);
                    }
                    qry_ += $"update eb_meeting_slots set is_approved = 'T' where  id = {request.Id}; ";
                }
                else if (MSD[0].IsApproved == "T" && MSD[0].MeetingId > 0)
                {
                    qry_ += $@"update eb_meeting_slot_participants set confirmation = 1 where id = {CurrentUser.ParticipantId} ;";
                    qry_ += Insert_Participants(MSD[0].MeetingId, CurrentUser.ParticipantId);
                }
                else if (MSD[0].IsApproved == "F")
                {
                    qry_ += $@"update eb_meeting_slot_participants set confirmation = 1 where id = {CurrentUser.ParticipantId} ; ";
                }

                if (CurrentUser.ParticipantType == 2 && MSD[0].MaxAttendees == (SPC.SlotAttendeeCount + 1))
                {
                    qry_ += Insert_Myaction(MyActionObj, request.Id, request.UserInfo.UserId, request.MyActionId, 2);
                }
                else
                {
                    qry_ += Insert_Myaction(MyActionObj, request.Id, request.UserInfo.UserId, request.MyActionId, 2);
                    qry_ += Insert_Myaction(MyActionObj, request.Id, request.UserInfo.UserId, request.MyActionId, 1);
                }
            }
            return qry_;
        }
        public string Insert_EbMeetings(int slot_id, int user_id)
        {
            string qry_ = $@"insert into eb_meetings (eb_meeting_slots_id, eb_created_at, eb_created_by) values({slot_id}, now(), {user_id});";
            return qry_;
        }
        public string Insert_Slot_Participants(User user, int scheduled_id, int slot_id)
        {
            string qry_ = $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({user.UserId}, 1, {scheduled_id}, {slot_id}, '{user.FullName}', '{user.Email}', 1, 1); ";
            return qry_;
        }
        public string Insert_Participants(int meetingid, int slot_participant_id)
        {
            string qry_ = "";
            if (meetingid == 0 && slot_participant_id == 0)
            {
                qry_ += $@"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id ) values 
                (eb_currval('eb_meetings_id_seq'), eb_currval('eb_meeting_slot_participants_id_seq'));";
            }
            else if (meetingid == 0 && slot_participant_id > 0)
            {
                qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) values ( eb_currval('eb_meetings_id_seq'),{slot_participant_id}); ";

            }
            else if (meetingid > 0 && slot_participant_id == 0)
            {
                qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id ) values ({meetingid}, eb_currval('eb_meeting_slot_participants_id_seq'));";
            }
            else if (meetingid > 0 && slot_participant_id > 0)
            {
                qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id ) values ({meetingid}, {slot_participant_id});";
            }
            return qry_;
        }
        public string Insert_Myaction(List<MyAction> MyActionObj, int slotid, int userid, int myactionid, int QueryOpts)
        {
            string qry_ = "";
            if (QueryOpts == 1)
            {
                qry_ += $@"insert into eb_my_actions (user_ids,usergroup_id,role_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_slots_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].UserIds}',{MyActionObj[0].UserGroupId},'{MyActionObj[0].RoleIds}',
                        NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',{slotid},
                         'F','F' ,'{userid},{MyActionObj[0].ExceptUserIds}');";
            }
            else if (QueryOpts == 2)
            {
                qry_ += $"update eb_my_actions set completed_at = now(), completed_by ={userid} , is_completed='T' where eb_meeting_slots_id = {slotid} " +
                      $"and id= {myactionid};";
            }
            return qry_;
        }

        public MeetingCancelByHostResponse Post(MeetingCancelByHostRequest request)
        {
            string query = @"
             select id, user_ids,usergroup_id,role_ids, form_ref_id, form_data_id , description, expiry_datetime, eb_meeting_slots_id ,except_user_ids from eb_my_actions 
            where eb_meeting_slots_id = {0} and id= {1} and is_completed='F';
                                        ";
            List<MyAction> MyActionObj = new List<MyAction>();
            MeetingCancelByHostResponse Resp = new MeetingCancelByHostResponse();
            string qry_ = "";
            try
            {
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query);
                int capacity1 = dt.Rows.Count;
                for (int i = 0; i < capacity1; i++)
                {
                    MyActionObj.Add(new MyAction()
                    {
                        Id = Convert.ToInt32(dt.Rows[i]["id"]),
                        SlotId = Convert.ToInt32(dt.Rows[i]["eb_meeting_slots_id"]),
                        Description = Convert.ToString(dt.Rows[i]["description"]),
                        UserIds = Convert.ToString(dt.Rows[i]["user_ids"]),
                        RoleIds = Convert.ToString(dt.Rows[i]["role_ids"]),
                        FormRefId = Convert.ToString(dt.Rows[i]["form_ref_id"]),
                        ExpiryDateTime = Convert.ToString(dt.Rows[i]["expiry_datetime"]),
                        ExceptUserIds = Convert.ToString(dt.Rows[i]["except_user_ids"]),
                        UserGroupId = Convert.ToInt32(dt.Rows[i]["usergroup_id"]),
                        FormDataId = Convert.ToInt32(dt.Rows[i]["form_data_id"]),
                    });
                }
                qry_ += $"update eb_my_actions set completed_at = now(), completed_by ={request.UserInfo.UserId} , is_completed='T' where eb_meeting_slots_id = {request.SlotId} " +
                      $"and id= {request.MyActionId};";
                qry_ += $@"insert into eb_my_actions (user_ids,usergroup_id,role_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_slots_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].UserIds}',{MyActionObj[0].UserGroupId},'{MyActionObj[0].RoleIds}',
                        NOW(),{MyActionObj[0].FormRefId}, {request.SlotId}, {MyActionObj[0].Description},'{MyActionTypes.Meeting}',
                        {MyActionObj[0].Description} , 'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";

                int a = this.EbConnectionFactory.DataDB.DoNonQuery(qry_);
                Resp.ResponseStatus = true;

            }
            catch (Exception e)
            {
                Resp.ResponseStatus = false;
                Console.WriteLine(e.StackTrace, e.Message);
            }
            return Resp;
        }
        public MeetingRejectByHostResponse Post(MeetingRejectByHostRequest request)
        {

            return new MeetingRejectByHostResponse();
        }


        // Get meeting details from my action
        public GetMeetingDetailsResponse Post(GetMeetingDetailRequest request)
        {
            GetMeetingDetailsResponse Resp = new GetMeetingDetailsResponse();
            string _qry = $@"
                 SELECT 
	           A.id, A.eb_meeting_slots_id, B.id as slot_id,C.id as meeting_schedule_id,C.description,B.time_from,B.time_to,
				C.meeting_date ,C.venue,C.integration,C.title,
				D.user_id , D.type_of_user,D.participant_type,E.fullname,E.email
			        FROM
					(select id , eb_meeting_slots_id from eb_meetings where id= {request.MeetingId} and eb_del = 'F' )A
						LEFT JOIN
							 (SELECT id , eb_meeting_schedule_id,time_from,time_to FROM  eb_meeting_slots)B
							 ON B.id = A.eb_meeting_slots_id		
							 LEFT JOIN	
							 (SELECT id ,title, meeting_date,venue,integration,description FROM  eb_meeting_schedule )C
							 ON C.id = B.eb_meeting_schedule_id	
							 LEFT JOIN	
							 (SELECT id , approved_slot_id, eb_meeting_schedule_id , user_id ,type_of_user,participant_type  FROM  eb_meeting_slot_participants )D
							 ON D.approved_slot_id = B.id and D.participant_type = 1
							 LEFT JOIN	
							 (select id, fullname,email from eb_users where eb_del = 'F')E
							 ON E.id = D.user_id;   
                 SELECT 
	           A.id, A.eb_meeting_slots_id, B.id as slot_id,C.id as meeting_schedule_id,C.description,B.time_from,B.time_to,
				C.meeting_date ,C.venue,C.integration,C.title,
				D.user_id , D.type_of_user,D.participant_type,E.fullname,E.email
			        FROM
					(select id , eb_meeting_slots_id from eb_meetings where id= {request.MeetingId} and eb_del = 'F' )A
						LEFT JOIN
							 (SELECT id , eb_meeting_schedule_id,time_from,time_to FROM  eb_meeting_slots)B
							 ON B.id = A.eb_meeting_slots_id		
							 LEFT JOIN	
							 (SELECT id ,title, meeting_date,venue,integration,description FROM  eb_meeting_schedule )C
							 ON C.id = B.eb_meeting_schedule_id	
							 LEFT JOIN	
							 (SELECT id , approved_slot_id, eb_meeting_schedule_id , user_id ,type_of_user,participant_type  FROM  eb_meeting_slot_participants )D
							 ON D.approved_slot_id = B.id and D.participant_type = 2
							 LEFT JOIN	
							 (select id, fullname,email from eb_users where eb_del = 'F')E
							 ON E.id = D.user_id; 
";

            try
            {
                EbDataSet ds = this.EbConnectionFactory.DataDB.DoQueries(_qry);
                int capacity1 = ds.Tables[0].Rows.Count;
                for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
                {
                    Resp.MeetingRequest.Add(
                        new MeetingRequest()
                        {
                            MeetingId = Convert.ToInt32(ds.Tables[0].Rows[i]["id"]),
                            Slotid = Convert.ToInt32(ds.Tables[0].Rows[i]["slot_id"]),
                            MeetingScheduleid = Convert.ToInt32(ds.Tables[0].Rows[i]["meeting_schedule_id"]),
                            Description = Convert.ToString(ds.Tables[0].Rows[i]["description"]),
                            TimeFrom = Convert.ToString(ds.Tables[0].Rows[i]["time_from"]),
                            TimeTo = Convert.ToString(ds.Tables[0].Rows[i]["time_to"]),
                            Title = Convert.ToString(ds.Tables[0].Rows[i]["title"]),
                            MeetingDate = Convert.ToString(ds.Tables[0].Rows[0]["meeting_date"]),
                            Venue = Convert.ToString(ds.Tables[0].Rows[i]["venue"]),
                            Integration = Convert.ToString(ds.Tables[0].Rows[i]["integration"]),
                            Fullname = Convert.ToString(ds.Tables[0].Rows[i]["fullname"]),
                            UserId = Convert.ToInt32(ds.Tables[0].Rows[i]["user_id"]),
                            TypeofUser = Convert.ToInt32(ds.Tables[0].Rows[i]["type_of_user"]),
                            ParticipantType = Convert.ToInt32(ds.Tables[0].Rows[i]["participant_type"]),
                            Email = Convert.ToString(ds.Tables[0].Rows[i]["email"]),
                        });
                }
                for (int i = 0; i < ds.Tables[1].Rows.Count; i++)
                {
                    Resp.MeetingRequest.Add(
                        new MeetingRequest()
                        {
                            MeetingId = Convert.ToInt32(ds.Tables[1].Rows[i]["id"]),
                            Slotid = Convert.ToInt32(ds.Tables[1].Rows[i]["slot_id"]),
                            MeetingScheduleid = Convert.ToInt32(ds.Tables[1].Rows[i]["meeting_schedule_id"]),
                            Description = Convert.ToString(ds.Tables[1].Rows[i]["description"]),
                            TimeFrom = Convert.ToString(ds.Tables[1].Rows[i]["time_from"]),
                            TimeTo = Convert.ToString(ds.Tables[1].Rows[i]["time_to"]),
                            Title = Convert.ToString(ds.Tables[1].Rows[i]["title"]),
                            MeetingDate = Convert.ToString(ds.Tables[1].Rows[0]["meeting_date"]),
                            Venue = Convert.ToString(ds.Tables[1].Rows[i]["venue"]),
                            Integration = Convert.ToString(ds.Tables[1].Rows[i]["integration"]),
                            Fullname = Convert.ToString(ds.Tables[1].Rows[i]["fullname"]),
                            UserId = Convert.ToInt32(ds.Tables[1].Rows[i]["user_id"]),
                            TypeofUser = Convert.ToInt32(ds.Tables[1].Rows[i]["type_of_user"]),
                            ParticipantType = Convert.ToInt32(ds.Tables[1].Rows[i]["participant_type"]),
                            Email = Convert.ToString(ds.Tables[1].Rows[i]["email"]),
                            FormDataId = Convert.ToInt32(ds.Tables[1].Rows[i]["form_data_id"]),
                            FormRefid = Convert.ToString(ds.Tables[1].Rows[i]["form_refid"]),
                        });
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace, e.Message);
            }
            return Resp;
        }

        public PickMeetingSLotResponse Post(PickMeetingSLotRequest request)
        {
            string query = @"
            SELECT 
		     A.id as slot_id , A.eb_meeting_schedule_id, A.is_approved,
			 B.no_of_attendee, B.no_of_hosts,B.max_hosts,B.max_attendees,  COALESCE (B.meeting_opts, 0) as meeting_opts,
			 COALESCE (D.id, 0) as meeting_id
	            FROM
				(SELECT 
						id, eb_meeting_schedule_id , is_approved, 
					meeting_date, time_from, time_to
	                     FROM 
		                     eb_meeting_slots 
	                     WHERE 
		                     eb_del = 'F' and id = {0})A
						LEFT JOIN	 
							 (SELECT id, no_of_attendee, no_of_hosts,max_hosts,max_attendees ,meeting_opts FROM  eb_meeting_schedule)B
							 ON
 	                     B.id = A.eb_meeting_schedule_id	
                     LEFT JOIN 
                     (SELECT 
		                     id, eb_meeting_slots_id
	                     FROM 
		                     eb_meetings
	                     where
		                     eb_del = 'F') D
		                     ON
 	                     D.eb_meeting_slots_id = A.id ; 
 						select count(*) as slot_attendee_count from eb_meeting_slot_participants where approved_slot_id = {0} 
									   and participant_type=2 and confirmation = 1;
						select count(*) as slot_host_count from eb_meeting_slot_participants where approved_slot_id = {0} 
									   and participant_type=1 and confirmation = 1;
            select id, user_ids,usergroup_id,role_ids, form_ref_id, form_data_id , description, expiry_datetime, eb_meeting_slots_id,eb_meeting_schedule_id ,except_user_ids from eb_my_actions 
            where id= {1} and is_completed='F';

select COALESCE(A.user_ids ,'') as user_ids,COALESCE(A.role_ids ,'') as role_ids,A.usergroup_id,A.eb_meeting_schedule_id ,
B.participant_type , B.type_of_user from 
 ( select id, COALESCE(user_ids ,'') as user_ids,COALESCE(role_ids ,'') as role_ids,COALESCE(usergroup_id , 0)  usergroup_id,eb_meeting_schedule_id,eb_meeting_slots_id 
		   from eb_my_actions where id = {1} and is_completed ='F') A
		   LEFT JOIN
		   (select COALESCE(user_ids ,'') as user_ids,COALESCE(role_ids ,'') as role_ids, COALESCE(user_group_id , 0) user_group_id,eb_meeting_schedule_id,participant_type ,type_of_user
			from eb_meeting_scheduled_participants where eb_del ='F' ) B
			ON B.eb_meeting_schedule_id = A.eb_meeting_schedule_id 
			and B.user_ids = A.user_ids and B.role_ids = A.role_ids and B.user_group_id = A.usergroup_id;

       SELECT 
		     A.id as slot_id , A.eb_meeting_schedule_id,
			 COALESCE (B.id, 0) as participant_id,B.participant_type,B.type_of_user,B.user_id,B.confirmation
	            FROM
				(SELECT id, eb_meeting_schedule_id
	                     FROM  eb_meeting_slots 
	                     WHERE  eb_del = 'F' and id = {0})A
						LEFT JOIN	
						(SELECT id, user_id,eb_meeting_schedule_id,approved_slot_id ,type_of_user,participant_type,confirmation
	                     FROM eb_meeting_slot_participants
	                     GROUP BY
		                     id,user_id,eb_meeting_schedule_id, approved_slot_id, type_of_user,participant_type, eb_del,confirmation
	                     Having eb_del = 'F')B
                     ON B.eb_meeting_schedule_id = A.eb_meeting_schedule_id and B.approved_slot_id = A.id 
                         where participant_type is not null;    
                    select count(*)  from eb_meeting_slot_participants where approved_slot_id = {0} and user_id = {2}
                                        ";

            List<MyAction> MyActionObj = new List<MyAction>();

            List<MeetingScheduleDetails> MSD = new List<MeetingScheduleDetails>(); //MSD Meeting Schedule Details
            ScheduledParticipants SP = new ScheduledParticipants(); //SP Scheduled Participant
            List<SlotParticipantsDetails> SPL = new List<SlotParticipantsDetails>(); //SPL Slot Participant List
            SlotParticipantsDetails CurrentUser = new SlotParticipantsDetails(); //SPL Slot Participant List
            SlotParticipantCount SPC = new SlotParticipantCount(); //SPL Slot Participant Count


            PickMeetingSLotResponse Resp = new PickMeetingSLotResponse();

            Resp.ResponseStatus = true;
            try
            {
                String _query = string.Format(query, request.SlotId, request.MyActionId, request.UserInfo.UserId);
                EbDataSet ds = this.EbConnectionFactory.DataDB.DoQueries(_query);
                for (int k = 0; k < ds.Tables[0].Rows.Count; k++)
                {
                    MSD.Add(new MeetingScheduleDetails()
                    {
                        SlotId = Convert.ToInt32(ds.Tables[0].Rows[k]["slot_id"]),
                        MeetingScheduleId = Convert.ToInt32(ds.Tables[0].Rows[k]["eb_meeting_schedule_id"]),
                        MeetingId = Convert.ToInt32(ds.Tables[0].Rows[k]["meeting_id"]),
                        MinAttendees = Convert.ToInt32(ds.Tables[0].Rows[k]["no_of_attendee"]),
                        MinHosts = Convert.ToInt32(ds.Tables[0].Rows[k]["no_of_hosts"]),
                        MaxAttendees = Convert.ToInt32(ds.Tables[0].Rows[k]["max_attendees"]),
                        MaxHosts = Convert.ToInt32(ds.Tables[0].Rows[k]["max_hosts"]),
                        IsApproved = Convert.ToString(ds.Tables[0].Rows[k]["is_approved"]),
                        MeetingOpts = (MeetingOptions)Convert.ToInt32(ds.Tables[0].Rows[k]["meeting_opts"])
                    });
                }

                SPC.SlotAttendeeCount = Convert.ToInt32(ds.Tables[1].Rows[0]["slot_attendee_count"]);
                SPC.SlotHostCount = Convert.ToInt32(ds.Tables[2].Rows[0]["slot_host_count"]);
                for (int i = 0; i < ds.Tables[3].Rows.Count; i++)
                {
                    MyActionObj.Add(new MyAction()
                    {
                        Id = Convert.ToInt32(ds.Tables[3].Rows[i]["id"]),
                        SlotId = Convert.ToInt32(ds.Tables[3].Rows[i]["eb_meeting_slots_id"]),
                        MeetingScheduleId = Convert.ToInt32(ds.Tables[3].Rows[i]["eb_meeting_schedule_id"]),
                        Description = Convert.ToString(ds.Tables[3].Rows[i]["description"]),
                        UserIds = Convert.ToString(ds.Tables[3].Rows[i]["user_ids"]),
                        RoleIds = Convert.ToString(ds.Tables[3].Rows[i]["role_ids"]),
                        FormRefId = Convert.ToString(ds.Tables[3].Rows[i]["form_ref_id"]),
                        ExpiryDateTime = Convert.ToString(ds.Tables[3].Rows[i]["expiry_datetime"]),
                        ExceptUserIds = Convert.ToString(ds.Tables[3].Rows[i]["except_user_ids"]),
                        UserGroupId = Convert.ToInt32(ds.Tables[3].Rows[i]["usergroup_id"]),
                        FormDataId = Convert.ToInt32(ds.Tables[3].Rows[i]["form_data_id"]),

                    });
                }
                for (int i = 0; i < ds.Tables[4].Rows.Count; i++)
                {
                    SP.UserIds = Convert.ToString(ds.Tables[4].Rows[0]["user_ids"]);
                    SP.RoleIds = Convert.ToString(ds.Tables[4].Rows[0]["role_ids"]);
                    SP.UserGroupId = Convert.ToInt32(ds.Tables[4].Rows[0]["usergroup_id"]);
                    SP.MeetingScheduleId = Convert.ToInt32(ds.Tables[4].Rows[0]["eb_meeting_schedule_id"]);
                    SP.ParticipantType = Convert.ToInt32(ds.Tables[4].Rows[0]["participant_type"]);
                    SP.TypeOfUser = Convert.ToInt32(ds.Tables[4].Rows[0]["type_of_user"]);
                }
                for (int k = 0; k < ds.Tables[5].Rows.Count; k++)
                {
                    SPL.Add(new SlotParticipantsDetails()
                    {
                        SlotId = Convert.ToInt32(ds.Tables[5].Rows[k]["slot_id"]),
                        MeetingScheduleId = Convert.ToInt32(ds.Tables[5].Rows[k]["eb_meeting_schedule_id"]),
                        ParticipantId = Convert.ToInt32(ds.Tables[5].Rows[k]["participant_id"]),
                        ParticipantType = Convert.ToInt32(ds.Tables[5].Rows[k]["participant_type"]),
                        TypeOfUser = Convert.ToInt32(ds.Tables[5].Rows[k]["type_of_user"]),
                        UserId = Convert.ToInt32(ds.Tables[5].Rows[k]["user_id"]),
                        Confirmation = Convert.ToInt32(ds.Tables[5].Rows[k]["confirmation"]),
                    });
                }
                string qry_ = "";
                if (ds.Tables[4].Rows.Count > 0 && SP.ParticipantType == 1 && MSD[0].MaxHosts == 0 && Convert.ToInt32(ds.Tables[6].Rows[0]["count"]) == 0)
                {
                    if (MSD[0].IsApproved == "F" && MSD[0].MinHosts == (SPC.SlotHostCount + 1) && MSD[0].MinAttendees <= SPC.SlotAttendeeCount)
                    {
                        qry_ += $@"insert into eb_meetings (eb_meeting_slots_id, eb_created_by)
                            values({MSD[0].SlotId}, 1);
                        insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotId}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 1); ";
                        for (int k = 0; k < SPL.Count; k++)
                            qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) values ( eb_currval('eb_meetings_id_seq'),{SPL[k].ParticipantId}); ";
                        qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id ) values (eb_currval('eb_meetings_id_seq'), eb_currval('eb_meeting_slot_participants_id_seq'));";
                        qry_ += $"update eb_meeting_slots set is_approved = 'T' where  id = {request.SlotId}; ";
                    }
                    else if (MSD[0].IsApproved == "T")
                    {
                        qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotId}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 1);
                        insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) 
                            values({MSD[0].MeetingId}, eb_currval('eb_meeting_slot_participants_id_seq')); ";
                    }
                    else if (MSD[0].IsApproved == "F" && (SPC.SlotHostCount + 1) <= MSD[0].MinHosts)
                    {
                        qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotId}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 1);  ";
                    }
                    if (MyActionObj[0].UserIds != "")
                        qry_ += $@"insert into eb_my_actions (user_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].RoleIds}', NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";
                    else if (MyActionObj[0].RoleIds != "")
                        qry_ += $@"insert into eb_my_actions (role_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].RoleIds}',NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";
                    else if (MyActionObj[0].UserGroupId > 0)
                        qry_ += $@"insert into eb_my_actions (usergroup_id,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values({MyActionObj[0].UserGroupId},NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";

                    qry_ += $@"update eb_my_actions set completed_at = now(), completed_by ={request.UserInfo.UserId} , is_completed='T' where  
                        id= {request.MyActionId};";
                }
                else if (ds.Tables[4].Rows.Count > 0 && SP.ParticipantType == 1 && MSD[0].MaxHosts > SPC.SlotHostCount && Convert.ToInt32(ds.Tables[6].Rows[0]["count"]) == 0)
                {
                    if (MSD[0].IsApproved == "F" && MSD[0].MinHosts == (SPC.SlotHostCount + 1) && MSD[0].MinAttendees <= SPC.SlotAttendeeCount && MSD[0].MaxAttendees >= SPC.SlotAttendeeCount)
                    {
                        qry_ += $@"insert into eb_meetings (eb_meeting_slots_id, eb_created_by)
                            values({MSD[0].SlotId}, 1);
                        insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotId}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 1); ";
                        for (int k = 0; k < SPL.Count; k++)
                            qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) values ( eb_currval('eb_meetings_id_seq'),{SPL[k].ParticipantId}); ";
                        qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id ) values (eb_currval('eb_meetings_id_seq'), eb_currval('eb_meeting_slot_participants_id_seq'));";
                        qry_ += $"update eb_meeting_slots set is_approved = 'T' where  id = {request.SlotId}; ";
                    }
                    else if (MSD[0].IsApproved == "T")
                    {
                        qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotId}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 1);
                        insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) 
                            values({MSD[0].MeetingId}, eb_currval('eb_meeting_slot_participants_id_seq')); ";
                    }
                    else if (MSD[0].IsApproved == "F" && (SPC.SlotHostCount + 1) <= MSD[0].MinHosts)
                    {
                        qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotId}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 1);  ";
                    }

                    if (SP.ParticipantType == 1 && MSD[0].MaxHosts == (SPC.SlotHostCount + 1))
                    {
                        qry_ += $@"update eb_my_actions set completed_at = now(), completed_by ={request.UserInfo.UserId} , is_completed='T' where  id= {request.MyActionId};";
                    }
                    else
                    {
                        if (MyActionObj[0].UserIds != "")
                            qry_ += $@"insert into eb_my_actions (user_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].RoleIds}', NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";
                        else if (MyActionObj[0].RoleIds != "")
                            qry_ += $@"insert into eb_my_actions (role_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].RoleIds}',NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";
                        else if (MyActionObj[0].UserGroupId > 0)
                            qry_ += $@"insert into eb_my_actions (usergroup_id,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values({MyActionObj[0].UserGroupId},NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";

                        qry_ += $@"update eb_my_actions set completed_at = now(), completed_by ={request.UserInfo.UserId} , is_completed='T' where  
                        id= {request.MyActionId};";
                    }
                }
                else if (ds.Tables[4].Rows.Count > 0 && SP.ParticipantType == 2 && MSD[0].MaxAttendees == 0 && Convert.ToInt32(ds.Tables[6].Rows[0]["count"]) == 0)
                {
                    if (MSD[0].IsApproved == "F" && MSD[0].MinAttendees == (SPC.SlotAttendeeCount + 1) && MSD[0].MinHosts <= SPC.SlotHostCount)
                    {
                        qry_ += $@"insert into eb_meetings (eb_meeting_slots_id, eb_created_by)
                            values({MSD[0].SlotId}, 1);
                        insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotId}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 2); ";
                        for (int k = 0; k < SPL.Count; k++)
                            qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) values ( eb_currval('eb_meetings_id_seq'),{SPL[k].ParticipantId}); ";
                        qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id ) values (eb_currval('eb_meetings_id_seq'), eb_currval('eb_meeting_slot_participants_id_seq'));";
                        qry_ += $"update eb_meeting_slots set is_approved = 'T' where  id = {request.SlotId}; ";
                    }
                    else if (MSD[0].IsApproved == "T")
                    {
                        qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotId}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 2);
                        insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) 
                            values({MSD[0].MeetingId}, eb_currval('eb_meeting_slot_participants_id_seq')); ";
                    }
                    else if (MSD[0].IsApproved == "F" && (SPC.SlotAttendeeCount + 1) <= MSD[0].MinAttendees)
                    {
                        qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotId}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 2);  ";
                    }

                    if (MyActionObj[0].UserIds != "")
                        qry_ += $@"insert into eb_my_actions (user_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].RoleIds}', NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";
                    else if (MyActionObj[0].RoleIds != "")
                        qry_ += $@"insert into eb_my_actions (role_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].RoleIds}',NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";
                    else if (MyActionObj[0].UserGroupId > 0)
                        qry_ += $@"insert into eb_my_actions (usergroup_id,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values({MyActionObj[0].UserGroupId},NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";

                    qry_ += $@"update eb_my_actions set completed_at = now(), completed_by ={request.UserInfo.UserId} , is_completed='T' where  
                        id= {request.MyActionId};";
                }
                else if (ds.Tables[4].Rows.Count > 0 && SP.ParticipantType == 2 && MSD[0].MaxAttendees > SPC.SlotAttendeeCount && Convert.ToInt32(ds.Tables[6].Rows[0]["count"]) == 0)
                {
                    if (MSD[0].IsApproved == "F" && MSD[0].MinAttendees == (SPC.SlotAttendeeCount + 1) && MSD[0].MinHosts <= SPC.SlotHostCount && MSD[0].MaxHosts >= SPC.SlotHostCount)
                    {
                        qry_ += $@"insert into eb_meetings (eb_meeting_slots_id, eb_created_by)
                            values({MSD[0].SlotId}, 1);
                        insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotId}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 2); ";
                        for (int k = 0; k < SPL.Count; k++)
                            qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) values ( eb_currval('eb_meetings_id_seq'),{SPL[k].ParticipantId}); ";
                        qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id ) values (eb_currval('eb_meetings_id_seq'), eb_currval('eb_meeting_slot_participants_id_seq'));";
                        qry_ += $"update eb_meeting_slots set is_approved = 'T' where  id = {request.SlotId}; ";
                    }
                    else if (MSD[0].IsApproved == "T")
                    {
                        qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotId}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 2);
                        insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) 
                            values({MSD[0].MeetingId}, eb_currval('eb_meeting_slot_participants_id_seq')); ";
                    }
                    else if (MSD[0].IsApproved == "F" && (SPC.SlotAttendeeCount + 1) <= MSD[0].MinAttendees)
                    {
                        qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotId}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 2);  ";
                    }

                    if (SP.ParticipantType == 1 && MSD[0].MaxAttendees == (SPC.SlotAttendeeCount + 1))
                    {
                        qry_ += $@"update eb_my_actions set completed_at = now(), completed_by ={request.UserInfo.UserId} , is_completed='T' where  id= {request.MyActionId};";
                    }
                    else
                    {
                        if (MyActionObj[0].UserIds != "")
                            qry_ += $@"insert into eb_my_actions (user_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].RoleIds}', NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";
                        else if (MyActionObj[0].RoleIds != "")
                            qry_ += $@"insert into eb_my_actions (role_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].RoleIds}',NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";
                        else if (MyActionObj[0].UserGroupId > 0)
                            qry_ += $@"insert into eb_my_actions (usergroup_id,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values({MyActionObj[0].UserGroupId},NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";

                        qry_ += $@"update eb_my_actions set completed_at = now(), completed_by ={request.UserInfo.UserId} , is_completed='T' where  
                        id= {request.MyActionId};";
                    }
                }
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(qry_);

            }
            catch (Exception e)
            {
                Resp.ResponseStatus = false;
                Console.WriteLine(e.Message, e.StackTrace);
            }

            return Resp;
        }


        //pick multiple meeting slots in a single meeting schedule
        public PickMultipleMeetingResponse Post(PickMultipleMeetingRequest request)
        {

            PickMultipleMeetingResponse Resp = new PickMultipleMeetingResponse();
            string qry_ = "";
            for (int p = 0; p < request.SlotIds.Count; p++)
            {
                string query = @"
            SELECT 
		     A.id as slot_id , A.eb_meeting_schedule_id, A.is_approved,
			 B.no_of_attendee, B.no_of_hosts,B.max_hosts,B.max_attendees,  COALESCE (B.meeting_opts, 0) as meeting_opts,
			 COALESCE (D.id, 0) as meeting_id
	            FROM
				(SELECT 
						id, eb_meeting_schedule_id , is_approved, 
					meeting_date, time_from, time_to
	                     FROM 
		                     eb_meeting_slots 
	                     WHERE 
		                     eb_del = 'F' and id = {0})A
						LEFT JOIN	 
							 (SELECT id, no_of_attendee, no_of_hosts,max_hosts,max_attendees ,meeting_opts FROM  eb_meeting_schedule)B
							 ON
 	                     B.id = A.eb_meeting_schedule_id	
                     LEFT JOIN 
                     (SELECT 
		                     id, eb_meeting_slots_id
	                     FROM 
		                     eb_meetings
	                     where
		                     eb_del = 'F') D
		                     ON
 	                     D.eb_meeting_slots_id = A.id ; 
 						select count(*) as slot_attendee_count from eb_meeting_slot_participants where approved_slot_id = {0} 
									   and participant_type=2 and confirmation = 1;
						select count(*) as slot_host_count from eb_meeting_slot_participants where approved_slot_id = {0} 
									   and participant_type=1 and confirmation = 1;
            select id, user_ids,usergroup_id,role_ids, form_ref_id, form_data_id , description, expiry_datetime, eb_meeting_slots_id,eb_meeting_schedule_id ,except_user_ids from eb_my_actions 
            where id= {1} and is_completed='F';

select COALESCE(A.user_ids ,'') as user_ids,COALESCE(A.role_ids ,'') as role_ids,A.usergroup_id,A.eb_meeting_schedule_id ,
B.participant_type , B.type_of_user from 
 ( select id, COALESCE(user_ids ,'') as user_ids,COALESCE(role_ids ,'') as role_ids,COALESCE(usergroup_id , 0)  usergroup_id,eb_meeting_schedule_id,eb_meeting_slots_id 
		   from eb_my_actions where id = {1} and is_completed ='F') A
		   LEFT JOIN
		   (select COALESCE(user_ids ,'') as user_ids,COALESCE(role_ids ,'') as role_ids, COALESCE(user_group_id , 0) user_group_id,eb_meeting_schedule_id,participant_type ,type_of_user
			from eb_meeting_scheduled_participants where eb_del ='F' ) B
			ON B.eb_meeting_schedule_id = A.eb_meeting_schedule_id 
			and B.user_ids = A.user_ids and B.role_ids = A.role_ids and B.user_group_id = A.usergroup_id;

       SELECT 
		     A.id as slot_id , A.eb_meeting_schedule_id,
			 COALESCE (B.id, 0) as participant_id,B.participant_type,B.type_of_user,B.user_id,B.confirmation
	            FROM
				(SELECT id, eb_meeting_schedule_id
	                     FROM  eb_meeting_slots 
	                     WHERE  eb_del = 'F' and id = {0})A
						LEFT JOIN	
						(SELECT id, user_id,eb_meeting_schedule_id,approved_slot_id ,type_of_user,participant_type,confirmation
	                     FROM eb_meeting_slot_participants
	                     GROUP BY
		                     id,user_id,eb_meeting_schedule_id, approved_slot_id, type_of_user,participant_type, eb_del,confirmation
	                     Having eb_del = 'F')B
                     ON B.eb_meeting_schedule_id = A.eb_meeting_schedule_id and B.approved_slot_id = A.id 
                         where participant_type is not null;    
                    select count(*)  from eb_meeting_slot_participants where approved_slot_id = {0} and user_id = {2}
                                        ";

                List<MyAction> MyActionObj = new List<MyAction>();

                List<MeetingScheduleDetails> MSD = new List<MeetingScheduleDetails>(); //MSD Meeting Schedule Details
                ScheduledParticipants SP = new ScheduledParticipants(); //SP Scheduled Participant
                List<SlotParticipantsDetails> SPL = new List<SlotParticipantsDetails>(); //SPL Slot Participant List
                SlotParticipantsDetails CurrentUser = new SlotParticipantsDetails(); //SPL Slot Participant List
                SlotParticipantCount SPC = new SlotParticipantCount(); //SPL Slot Participant Count

                Resp.ResponseStatus = true;
                try
                {
                    String _query = string.Format(query, request.SlotIds[p], request.MyActionId, request.UserInfo.UserId);
                    EbDataSet ds = this.EbConnectionFactory.DataDB.DoQueries(_query);
                    for (int k = 0; k < ds.Tables[0].Rows.Count; k++)
                    {
                        MSD.Add(new MeetingScheduleDetails()
                        {
                            SlotId = Convert.ToInt32(ds.Tables[0].Rows[k]["slot_id"]),
                            MeetingScheduleId = Convert.ToInt32(ds.Tables[0].Rows[k]["eb_meeting_schedule_id"]),
                            MeetingId = Convert.ToInt32(ds.Tables[0].Rows[k]["meeting_id"]),
                            MinAttendees = Convert.ToInt32(ds.Tables[0].Rows[k]["no_of_attendee"]),
                            MinHosts = Convert.ToInt32(ds.Tables[0].Rows[k]["no_of_hosts"]),
                            MaxAttendees = Convert.ToInt32(ds.Tables[0].Rows[k]["max_attendees"]),
                            MaxHosts = Convert.ToInt32(ds.Tables[0].Rows[k]["max_hosts"]),
                            IsApproved = Convert.ToString(ds.Tables[0].Rows[k]["is_approved"]),
                            MeetingOpts = (MeetingOptions)Convert.ToInt32(ds.Tables[0].Rows[k]["meeting_opts"])
                        });
                    }

                    SPC.SlotAttendeeCount = Convert.ToInt32(ds.Tables[1].Rows[0]["slot_attendee_count"]);
                    SPC.SlotHostCount = Convert.ToInt32(ds.Tables[2].Rows[0]["slot_host_count"]);
                    for (int i = 0; i < ds.Tables[3].Rows.Count; i++)
                    {
                        MyActionObj.Add(new MyAction()
                        {
                            Id = Convert.ToInt32(ds.Tables[3].Rows[i]["id"]),
                            SlotId = Convert.ToInt32(ds.Tables[3].Rows[i]["eb_meeting_slots_id"]),
                            MeetingScheduleId = Convert.ToInt32(ds.Tables[3].Rows[i]["eb_meeting_schedule_id"]),
                            Description = Convert.ToString(ds.Tables[3].Rows[i]["description"]),
                            UserIds = Convert.ToString(ds.Tables[3].Rows[i]["user_ids"]),
                            RoleIds = Convert.ToString(ds.Tables[3].Rows[i]["role_ids"]),
                            FormRefId = Convert.ToString(ds.Tables[3].Rows[i]["form_ref_id"]),
                            ExpiryDateTime = Convert.ToString(ds.Tables[3].Rows[i]["expiry_datetime"]),
                            ExceptUserIds = Convert.ToString(ds.Tables[3].Rows[i]["except_user_ids"]),
                            UserGroupId = Convert.ToInt32(ds.Tables[3].Rows[i]["usergroup_id"]),
                            FormDataId = Convert.ToInt32(ds.Tables[3].Rows[i]["form_data_id"]),

                        });
                    }
                    for (int i = 0; i < ds.Tables[4].Rows.Count; i++)
                    {
                        SP.UserIds = Convert.ToString(ds.Tables[4].Rows[0]["user_ids"]);
                        SP.RoleIds = Convert.ToString(ds.Tables[4].Rows[0]["role_ids"]);
                        SP.UserGroupId = Convert.ToInt32(ds.Tables[4].Rows[0]["usergroup_id"]);
                        SP.MeetingScheduleId = Convert.ToInt32(ds.Tables[4].Rows[0]["eb_meeting_schedule_id"]);
                        SP.ParticipantType = Convert.ToInt32(ds.Tables[4].Rows[0]["participant_type"]);
                        SP.TypeOfUser = Convert.ToInt32(ds.Tables[4].Rows[0]["type_of_user"]);
                    }
                    for (int k = 0; k < ds.Tables[5].Rows.Count; k++)
                    {
                        SPL.Add(new SlotParticipantsDetails()
                        {
                            SlotId = Convert.ToInt32(ds.Tables[5].Rows[k]["slot_id"]),
                            MeetingScheduleId = Convert.ToInt32(ds.Tables[5].Rows[k]["eb_meeting_schedule_id"]),
                            ParticipantId = Convert.ToInt32(ds.Tables[5].Rows[k]["participant_id"]),
                            ParticipantType = Convert.ToInt32(ds.Tables[5].Rows[k]["participant_type"]),
                            TypeOfUser = Convert.ToInt32(ds.Tables[5].Rows[k]["type_of_user"]),
                            UserId = Convert.ToInt32(ds.Tables[5].Rows[k]["user_id"]),
                            Confirmation = Convert.ToInt32(ds.Tables[5].Rows[k]["confirmation"]),
                        });
                    }
                    if (ds.Tables[4].Rows.Count > 0 && SP.ParticipantType == 1 && MSD[0].MaxHosts == 0 && Convert.ToInt32(ds.Tables[6].Rows[0]["count"]) == 0)
                    {
                        if (MSD[0].IsApproved == "F" && MSD[0].MinHosts == (SPC.SlotHostCount + 1) && MSD[0].MinAttendees <= SPC.SlotAttendeeCount)
                        {
                            qry_ += $@"insert into eb_meetings (eb_meeting_slots_id, eb_created_by)
                            values({MSD[0].SlotId}, 1);
                        insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotIds[p]}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 1); ";
                            for (int k = 0; k < SPL.Count; k++)
                                qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) values ( eb_currval('eb_meetings_id_seq'),{SPL[k].ParticipantId}); ";
                            qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id ) values (eb_currval('eb_meetings_id_seq'), eb_currval('eb_meeting_slot_participants_id_seq'));";
                            qry_ += $"update eb_meeting_slots set is_approved = 'T' where  id = {request.SlotIds[p]}; ";
                        }
                        else if (MSD[0].IsApproved == "T")
                        {
                            qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotIds[p]}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 1);
                        insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) 
                            values({MSD[0].MeetingId}, eb_currval('eb_meeting_slot_participants_id_seq')); ";
                        }
                        else if (MSD[0].IsApproved == "F" && (SPC.SlotHostCount + 1) <= MSD[0].MinHosts)
                        {
                            qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotIds[p]}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 1);  ";
                        }
                        if (MyActionObj[0].UserIds != "")
                            qry_ += $@"insert into eb_my_actions (user_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].RoleIds}', NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";
                        else if (MyActionObj[0].RoleIds != "")
                            qry_ += $@"insert into eb_my_actions (role_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].RoleIds}',NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";
                        else if (MyActionObj[0].UserGroupId > 0)
                            qry_ += $@"insert into eb_my_actions (usergroup_id,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values({MyActionObj[0].UserGroupId},NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";

                        qry_ += $@"update eb_my_actions set completed_at = now(), completed_by ={request.UserInfo.UserId} , is_completed='T' where  
                        id= {request.MyActionId};";
                    }
                    else if (ds.Tables[4].Rows.Count > 0 && SP.ParticipantType == 1 && MSD[0].MaxHosts > SPC.SlotHostCount && Convert.ToInt32(ds.Tables[6].Rows[0]["count"]) == 0)
                    {
                        if (MSD[0].IsApproved == "F" && MSD[0].MinHosts == (SPC.SlotHostCount + 1) && MSD[0].MinAttendees <= SPC.SlotAttendeeCount && MSD[0].MaxAttendees >= SPC.SlotAttendeeCount)
                        {
                            qry_ += $@"insert into eb_meetings (eb_meeting_slots_id, eb_created_by)
                            values({MSD[0].SlotId}, 1);
                        insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotIds[p]}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 1); ";
                            for (int k = 0; k < SPL.Count; k++)
                                qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) values ( eb_currval('eb_meetings_id_seq'),{SPL[k].ParticipantId}); ";
                            qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id ) values (eb_currval('eb_meetings_id_seq'), eb_currval('eb_meeting_slot_participants_id_seq'));";
                            qry_ += $"update eb_meeting_slots set is_approved = 'T' where  id = {request.SlotIds[p]}; ";
                        }
                        else if (MSD[0].IsApproved == "T")
                        {
                            qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotIds[p]}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 1);
                        insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) 
                            values({MSD[0].MeetingId}, eb_currval('eb_meeting_slot_participants_id_seq')); ";
                        }
                        else if (MSD[0].IsApproved == "F" && (SPC.SlotHostCount + 1) <= MSD[0].MinHosts)
                        {
                            qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotIds[p]}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 1);  ";
                        }

                        if (SP.ParticipantType == 1 && MSD[0].MaxHosts == (SPC.SlotHostCount + 1))
                        {
                            qry_ += $@"update eb_my_actions set completed_at = now(), completed_by ={request.UserInfo.UserId} , is_completed='T' where  id= {request.MyActionId};";
                        }
                        else
                        {
                            if (MyActionObj[0].UserIds != "")
                                qry_ += $@"insert into eb_my_actions (user_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].RoleIds}', NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";
                            else if (MyActionObj[0].RoleIds != "")
                                qry_ += $@"insert into eb_my_actions (role_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].RoleIds}',NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";
                            else if (MyActionObj[0].UserGroupId > 0)
                                qry_ += $@"insert into eb_my_actions (usergroup_id,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values({MyActionObj[0].UserGroupId},NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";

                            qry_ += $@"update eb_my_actions set completed_at = now(), completed_by ={request.UserInfo.UserId} , is_completed='T' where  
                        id= {request.MyActionId};";
                        }
                    }
                    else if (ds.Tables[4].Rows.Count > 0 && SP.ParticipantType == 2 && MSD[0].MaxAttendees == 0 && Convert.ToInt32(ds.Tables[6].Rows[0]["count"]) == 0)
                    {
                        if (MSD[0].IsApproved == "F" && MSD[0].MinAttendees == (SPC.SlotAttendeeCount + 1) && MSD[0].MinHosts <= SPC.SlotHostCount)
                        {
                            qry_ += $@"insert into eb_meetings (eb_meeting_slots_id, eb_created_by)
                            values({MSD[0].SlotId}, 1);
                        insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotIds[p]}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 2); ";
                            for (int k = 0; k < SPL.Count; k++)
                                qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) values ( eb_currval('eb_meetings_id_seq'),{SPL[k].ParticipantId}); ";
                            qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id ) values (eb_currval('eb_meetings_id_seq'), eb_currval('eb_meeting_slot_participants_id_seq'));";
                            qry_ += $"update eb_meeting_slots set is_approved = 'T' where  id = {request.SlotIds[p]}; ";
                        }
                        else if (MSD[0].IsApproved == "T")
                        {
                            qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotIds[p]}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 2);
                        insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) 
                            values({MSD[0].MeetingId}, eb_currval('eb_meeting_slot_participants_id_seq')); ";
                        }
                        else if (MSD[0].IsApproved == "F" && (SPC.SlotAttendeeCount + 1) <= MSD[0].MinAttendees)
                        {
                            qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotIds[p]}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 2);  ";
                        }

                        if (MyActionObj[0].UserIds != "")
                            qry_ += $@"insert into eb_my_actions (user_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].RoleIds}', NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";
                        else if (MyActionObj[0].RoleIds != "")
                            qry_ += $@"insert into eb_my_actions (role_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].RoleIds}',NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";
                        else if (MyActionObj[0].UserGroupId > 0)
                            qry_ += $@"insert into eb_my_actions (usergroup_id,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values({MyActionObj[0].UserGroupId},NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";

                        qry_ += $@"update eb_my_actions set completed_at = now(), completed_by ={request.UserInfo.UserId} , is_completed='T' where  
                        id= {request.MyActionId};";
                    }
                    else if (ds.Tables[4].Rows.Count > 0 && SP.ParticipantType == 2 && MSD[0].MaxAttendees > SPC.SlotAttendeeCount && Convert.ToInt32(ds.Tables[6].Rows[0]["count"]) == 0)
                    {
                        if (MSD[0].IsApproved == "F" && MSD[0].MinAttendees == (SPC.SlotAttendeeCount + 1) && MSD[0].MinHosts <= SPC.SlotHostCount && MSD[0].MaxHosts >= SPC.SlotHostCount)
                        {
                            qry_ += $@"insert into eb_meetings (eb_meeting_slots_id, eb_created_by)
                            values({MSD[0].SlotId}, 1);
                        insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotIds[p]}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 2); ";
                            for (int k = 0; k < SPL.Count; k++)
                                qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) values ( eb_currval('eb_meetings_id_seq'),{SPL[k].ParticipantId}); ";
                            qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id ) values (eb_currval('eb_meetings_id_seq'), eb_currval('eb_meeting_slot_participants_id_seq'));";
                            qry_ += $"update eb_meeting_slots set is_approved = 'T' where  id = {request.SlotIds[p]}; ";
                        }
                        else if (MSD[0].IsApproved == "T")
                        {
                            qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotIds[p]}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 2);
                        insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) 
                            values({MSD[0].MeetingId}, eb_currval('eb_meeting_slot_participants_id_seq')); ";
                        }
                        else if (MSD[0].IsApproved == "F" && (SPC.SlotAttendeeCount + 1) <= MSD[0].MinAttendees)
                        {
                            qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.SlotIds[p]}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 2);  ";
                        }

                        if (SP.ParticipantType == 1 && MSD[0].MaxAttendees == (SPC.SlotAttendeeCount + 1))
                        {
                            qry_ += $@"update eb_my_actions set completed_at = now(), completed_by ={request.UserInfo.UserId} , is_completed='T' where  id= {request.MyActionId};";
                        }
                        else
                        {
                            if (MyActionObj[0].UserIds != "")
                                qry_ += $@"insert into eb_my_actions (user_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].RoleIds}', NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";
                            else if (MyActionObj[0].RoleIds != "")
                                qry_ += $@"insert into eb_my_actions (role_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].RoleIds}',NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";
                            else if (MyActionObj[0].UserGroupId > 0)
                                qry_ += $@"insert into eb_my_actions (usergroup_id,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_schedule_id,
                        is_completed,eb_del , except_user_ids)
                        values({MyActionObj[0].UserGroupId},NOW(),'{MyActionObj[0].FormRefId}',{MyActionObj[0].FormDataId}, '{MyActionObj[0].Description}','{MyActionTypes.Meeting}',
                        {MyActionObj[0].MeetingScheduleId},'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";

                            qry_ += $@"update eb_my_actions set completed_at = now(), completed_by ={request.UserInfo.UserId} , is_completed='T' where  
                        id= {request.MyActionId};";
                        }
                    }


                }
                catch (Exception e)
                {
                    Resp.ResponseStatus = false;
                    Console.WriteLine(e.Message, e.StackTrace);
                }
            }
            try
            {
                Resp.ResponseStatus = true;
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(qry_);
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message, e.StackTrace);
                Resp.ResponseStatus = false;
            }
            return Resp;
        }
        public ParticipantsListResponse Post(ParticipantsListRequest request)
        {
            ParticipantsListResponse Resp = new ParticipantsListResponse();
            //string qry_ = @"select id , fullname from eb_users;
            //        select id,role_name from eb_roles;
            //        select id , name from eb_usergroup;";
            string qry_ = "";
            for (int i = 0; i < 2; i++)
            {
                if (request.MeetingConfig[i].MeetingConfig == UsersType.Role && request.MeetingConfig[0].MeetingRoles.Count != 0)
                {
                    string temp = String.Join(",", request.MeetingConfig[0].MeetingRoles);
                    qry_ += $@"select id,role_name as name from eb_roles where id in({temp});";
                }
                else if (request.MeetingConfig[i].MeetingConfig == UsersType.UserGroup)
                {
                    string temp = String.Join(",", request.MeetingConfig[0].MeetingUserGroup);
                    qry_ += $@"select id , name as name from eb_usergroup where id in({temp});";
                }
                else if (request.MeetingConfig[i].MeetingConfig == UsersType.Users)
                {
                    if (request.MeetingConfig[i].MeetingUsers.Code != "")
                        qry_ += request.MeetingConfig[i].MeetingUsers.Code + ";";
                    else
                        qry_ += $@"select id , fullname as name from eb_users;";
                }
                else if (request.MeetingConfig[i].MeetingConfig == UsersType.Contact)
                {
                    //if (request.MeetingConfig[i].Contacts.Code != "")
                    //    qry_ += request.MeetingConfig[i].Contacts.Code + ";";
                    if (request.MeetingConfig[i].ContactFilter != "")
                        qry_ += $@"select name,id from eb_contacts where form_refid ='{request.MeetingConfig[i].ContactFilter}';";
                }
            }

            try
            {
                EbDataSet ds = this.EbConnectionFactory.DataDB.DoQueries(qry_);
                for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
                {
                    Resp.HostParticipantsList.Add(new Participants()
                    {
                        Id = Convert.ToInt32(ds.Tables[0].Rows[i]["id"]),
                        Name = Convert.ToString(ds.Tables[0].Rows[i]["name"]),
                        Type = request.MeetingConfig[0].MeetingConfig
                    });
                }
                for (int i = 0; i < ds.Tables[1].Rows.Count; i++)
                {
                    Resp.AttendeeParticipantsList.Add(new Participants()
                    {
                        Id = Convert.ToInt32(ds.Tables[1].Rows[i]["id"]),
                        Name = Convert.ToString(ds.Tables[1].Rows[i]["name"]),
                        Type = request.MeetingConfig[1].MeetingConfig
                    });
                }
                //for (int i = 0; i < ds.Tables[2].Rows.Count; i++)
                //{
                //    Resp.ParticipantsList.Add(new Participants()
                //    {
                //        Id = Convert.ToInt32(ds.Tables[2].Rows[i]["id"]),
                //        Name = Convert.ToString(ds.Tables[2].Rows[i]["name"]) + " - GrOUP",
                //        Type = UsersType.UserGroupId,
                //    });
                //}
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message, e.StackTrace);
            }
            return Resp;
        }

        public ParticipantsListAjaxResponse Post(ParticipantsListAjaxRequest request)
        {
            string query = $@"select u.id, u.fullname from eb_users u where not exists (select A.id,A.user_id,B.meeting_date,B.time_from,B.time_to from
            (select id,user_id,approved_slot_id from eb_meeting_slot_participants)A
            left join 
            (select id,meeting_date,time_from,time_to from eb_meeting_slots) B
            ON B.id = A.approved_slot_id and B.time_from BETWEEN '09:30:00' and '09:59:00' AND B.time_to BETWEEN '09:30:00' and '09:59:00'
            and meeting_date ='2020-05-07'
            where B.time_from is not null and u.id = user_id );";

            return new ParticipantsListAjaxResponse();
        }
    }
}
