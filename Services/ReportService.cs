using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Objects;
using ExpressBase.Objects.ReportRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.ServiceStack.MQServices;
using iTextSharp.text;
using iTextSharp.text.pdf;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    public class ReportService : EbBaseService
    {
        private DataSourceColumnsResponse cresp = null;
        private DataSourceDataResponse dresp = null;

        public EbReport Report = null;
        private iTextSharp.text.Font f = FontFactory.GetFont(FontFactory.HELVETICA, 12);
        private PdfContentByte canvas;
        private PdfWriter writer;
        private Document d;
        public PdfReader pdfReader;
        public PdfStamper stamp;
        public MemoryStream ms1;

        private float rh_Yposition;
        private float rf_Yposition;
        private float pf_Yposition;
        private float ph_Yposition;
        private float dt_Yposition;
        private float detailprintingtop = 0;

        public ReportService(ITenantDbFactory _dbf) : base(_dbf) { }
        public ReportRenderResponse Get(ReportRenderRequest request)
        {
            var myObjectservice = base.ResolveService<EbObjectService>();
            var resultlist = (EbObjectParticularVersionResponse)myObjectservice.Get(new EbObjectParticularVersionRequest { RefId = request.Refid });
            Report = EbSerializers.Json_Deserialize<EbReport>(resultlist.Data[0].Json);
            Report.IsLastpage = false;
            Report.watermarkImages = new Dictionary<string, byte[]>();
            Report.WaterMarkList = new List<object>();
            var myDataSourceservice = base.ResolveService<DataSourceService>();
            if (Report.DataSourceRefId != string.Empty)
            {
                cresp = this.Redis.Get<DataSourceColumnsResponse>(string.Format("{0}_columns", Report.DataSourceRefId));
                if (cresp.IsNull)
                    cresp = (DataSourceColumnsResponse)myDataSourceservice.Any(new DataSourceColumnsRequest { RefId = Report.DataSourceRefId });

                Report.DataColumns = (cresp.Columns.Count > 1) ? cresp.Columns[1] : cresp.Columns[0];

                dresp = (DataSourceDataResponse)myDataSourceservice.Any(new DataSourceDataRequest { RefId = Report.DataSourceRefId, Draw = 1, Start = 0, Length = 100 });
                Report.DataRow = dresp.Data;
            }
            iTextSharp.text.Rectangle rec = new iTextSharp.text.Rectangle(Report.Width, Report.Height);
            d = new Document(rec);
            ms1 = new MemoryStream();
            writer = PdfWriter.GetInstance(d, ms1);
            writer.Open();
            d.Open();
            //   writer.PageEvent = new HeaderFooter(this);
            writer.CloseStream = true;//important
            canvas = writer.DirectContent;
            Report.PageNumber = writer.PageNumber;
            Report.InitializeSummaryFields();
            GetWatermarkImages();
            iTextSharp.text.Font link = FontFactory.GetFont("Arial", 12, iTextSharp.text.Font.UNDERLINE, BaseColor.DarkGray);
            Anchor anchor = new Anchor("xyz", link);
            anchor.Reference = "http://eb_roby_dev.localhost:5000/ReportRender?refid=eb_roby_dev-eb_roby_dev-3-1127-1854?tab=" + JsonConvert.SerializeObject(Report.DataRow[Report.SerialNumber - 1]);
            d.Add(anchor);
            d.NewPage();

            DrawReportHeader();
            DrawDetail();
            d.Close();
            ms1.Position = 0;//important
            return new ReportRenderResponse { StreamWrapper = new MemorystreamWrapper(ms1) };

        }
        public void GetWatermarkImages()
        {
            var myFileService = base.ResolveService<FileService>();
            byte[] fileByte = null;
            if (Report.ReportObjects != null)
            {
                foreach (var field in Report.ReportObjects)
                {
                    if ((field as EbWaterMark).Image != string.Empty)
                    {
                        fileByte = myFileService.Post
                      (new DownloadFileRequest
                      {
                          FileDetails = new FileMeta
                          {
                              FileName = (field as EbWaterMark).Image + ".jpg",
                              FileType = "jpg"
                          }
                      });
                    }
                    Report.watermarkImages.Add((field as EbWaterMark).Image, fileByte);
                }
            }
        }
        public void DrawReportHeader()
        {
            rh_Yposition = 0;
            detailprintingtop = 0;
            foreach (EbReportHeader r_header in Report.ReportHeaders)
            {
                foreach (EbReportField field in r_header.Fields)
                {
                    DrawFields(field, rh_Yposition, 1);
                }
                rh_Yposition += r_header.Height;
            }
        }
        public void DrawPageHeader()
        {
            detailprintingtop = 0;
            ph_Yposition = (Report.PageNumber == 1) ? Report.ReportHeaderHeight : 0;
            foreach (EbPageHeader p_header in Report.PageHeaders)
            {
                foreach (EbReportField field in p_header.Fields)
                {
                    DrawFields(field, ph_Yposition, 1);
                }
                ph_Yposition += p_header.Height;
            }
        }

        public void DrawDetail()
        {
            if (Report.DataRow != null)
            {
                for (Report.SerialNumber = 1; Report.SerialNumber <= Report.DataRow.Count; Report.SerialNumber++)
                {
                    if (detailprintingtop < Report.DT_FillHeight && Report.DT_FillHeight - detailprintingtop >= Report.DetailHeight)
                    {
                        DoLoopInDetail(Report.SerialNumber);
                    }
                    else
                    {
                        detailprintingtop = 0;
                        d.NewPage();
                        Report.PageNumber = writer.PageNumber;
                        DoLoopInDetail(Report.SerialNumber);
                    }
                }
                if (Report.SerialNumber - 1 == Report.DataRow.Count)
                {
                    Report.IsLastpage = true;
                    // Report.CalculateDetailHeight(Report.IsLastpage, __datarows, Report.PageNumber);
                }
            }
            else
            {
                Report.IsLastpage = true;
                DoLoopInDetail(0);
            }
        }

        public void DoLoopInDetail(int serialnumber)
        {
            ph_Yposition = (Report.PageNumber == 1) ? Report.ReportHeaderHeight : 0;
            dt_Yposition = ph_Yposition + Report.PageHeaderHeight;
            foreach (EbReportDetail detail in Report.Detail)
            {
                foreach (EbReportField field in detail.Fields)
                {
                    DrawFields(field, dt_Yposition, serialnumber);
                }
                detailprintingtop += detail.Height;
            }
        }

        public void DrawPageFooter()
        {
            detailprintingtop = 0;
            ph_Yposition = (Report.PageNumber == 1) ? Report.ReportHeaderHeight : 0;
            dt_Yposition = ph_Yposition + Report.PageHeaderHeight;
            pf_Yposition = dt_Yposition + Report.DT_FillHeight;
            foreach (EbPageFooter p_footer in Report.PageFooters)
            {
                foreach (EbReportField field in p_footer.Fields)
                {
                    DrawFields(field, pf_Yposition, 1);
                }
                pf_Yposition += p_footer.Height;
            }
        }

        public void DrawReportFooter()
        {
            detailprintingtop = 0;
            dt_Yposition = ph_Yposition + Report.PageHeaderHeight;
            pf_Yposition = dt_Yposition + Report.DT_FillHeight;
            rf_Yposition = pf_Yposition + Report.PageFooterHeight;
            foreach (EbReportFooter r_footer in Report.ReportFooters)
            {
                foreach (EbReportField field in r_footer.Fields)
                {
                    DrawFields(field, rf_Yposition, 1);
                }
                rf_Yposition += r_footer.Height;
            }
        }
        public void DrawFields(EbReportField field, float section_Yposition, int serialnumber)
        {
            var column_name = string.Empty;
            var column_val = string.Empty;
            if (Report.PageSummaryFields.ContainsKey(field.Title) || Report.ReportSummaryFields.ContainsKey(field.Title))
                Report.CallSummerize(field.Title, serialnumber);
            if (field is EbDataField)
            {
                if (field is IEbDataFieldSummary)
                {
                    column_val = (field as IEbDataFieldSummary).SummarizedValue.ToString();
                }
                else
                {
                    var table = field.Title.Split('.')[0];
                    column_name = field.Title.Split('.')[1];
                    column_val = Report.GeFieldtData(column_name, serialnumber);
                }
                field.DrawMe(canvas, Report.Height, section_Yposition, detailprintingtop, column_val);
            }
            if ((field is EbPageNo) || (field is EbPageXY) || (field is EbDateTime) || (field is EbSerialNumber))
            {
                if (field is EbPageNo)
                    column_val = Report.PageNumber.ToString();
                else if (field is EbPageXY)
                    column_val = Report.PageNumber + "/"/* + writer.PageCount*/;
                else if (field is EbDateTime)
                    column_val = DateTime.Now.ToString();
                else if (field is EbSerialNumber)
                    column_val = Report.SerialNumber.ToString();
                field.DrawMe(canvas, Report.Height, section_Yposition, detailprintingtop, column_val);
            }
            else if (field is EbImg)
            {
                var myFileService = base.ResolveService<FileService>();
                byte[] fileByte = myFileService.Post
                     (new DownloadFileRequest
                     {
                         FileDetails = new FileMeta
                         {
                             FileName = (field as EbImg).Image + ".jpg",
                             FileType = "jpg"
                         }
                     });
                field.DrawMe(d, fileByte);
            }
            else if ((field is EbText) || (field is EbReportFieldShape))
            {
                field.DrawMe(canvas, Report.Height, section_Yposition, detailprintingtop);
            }
        }

    }
}
