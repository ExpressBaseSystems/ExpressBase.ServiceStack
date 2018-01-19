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
        EbReport Report = null;
        public ReportService(ITenantDbFactory _dbf) : base(_dbf) { }
        public ReportRenderResponse Get(ReportRenderRequest request)
        {
            DataSourceColumnsResponse cresp = null;
            DataSourceDataResponse dresp = null;
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
            Document d = new Document(rec);
            MemoryStream ms1 = new MemoryStream();
            PdfWriter writer = PdfWriter.GetInstance(d, ms1);
            writer.Open();
            d.Open();
            //   writer.PageEvent = new HeaderFooter(this);
            writer.CloseStream = true;//important
            PdfContentByte canvas = writer.DirectContent;
            Report.PageNumber = writer.PageNumber;
            Report.InitializeSummaryFields();
            GetWatermarkImages();
            iTextSharp.text.Font link = FontFactory.GetFont("Arial", 12, iTextSharp.text.Font.UNDERLINE, BaseColor.DarkGray);
            Anchor anchor = new Anchor("xyz", link);
            anchor.Reference = "http://eb_roby_dev.localhost:5000/ReportRender?refid=eb_roby_dev-eb_roby_dev-3-1127-1854?tab=" + JsonConvert.SerializeObject(Report.DataRow[Report.SerialNumber - 1]);
            d.Add(anchor);
            d.NewPage();

            //DrawReportHeader();
            //DrawDetail();
            d.Close();
            ms1.Position = 0;//important
            Console.WriteLine(">>>>>>> Len: " + ms1.Length + "\n");
            return new ReportRenderResponse{ MemoryStream = new MemorystreamWrapper(ms1) };
          //return new FileStreamResult(ms1, "application/pdf");
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
                        fileByte =myFileService.Post
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


    }
}
