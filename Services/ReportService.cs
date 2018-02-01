using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.EbServiceStack.ReqNRes;
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
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using QRCoder;

namespace ExpressBase.ServiceStack
{
    public class ReportService : EbBaseService
    {
        private DataSourceColumnsResponse cresp = null;
        private DataSourceDataResponse dresp = null;

        public EbReport Report = null;
        private iTextSharp.text.Font f = FontFactory.GetFont(FontFactory.HELVETICA, 12);
        public ReportService(IEbConnectionFactory _dbf) : base(_dbf) { }

        public ReportRenderResponse Get(ReportRenderRequest request)
        {
            //-- Get REPORT object and Init 
            var myObjectservice = base.ResolveService<EbObjectService>();
            EbObjectParticularVersionResponse resultlist = myObjectservice.Get(new EbObjectParticularVersionRequest { RefId = request.Refid }) as EbObjectParticularVersionResponse;
            Report = EbSerializers.Json_Deserialize<EbReport>(resultlist.Data[0].Json);
            Report.ReportService = this;
            Report.FileService = base.ResolveService<FileService>();
            Report.SolutionId = request.TenantAccountId;
            Report.IsLastpage = false;
            Report.watermarkImages = new Dictionary<string, byte[]>();
            Report.WaterMarkList = new List<object>();
            Report.CurrentTimestamp = DateTime.Now;
            //-- END REPORT object INIT

            var myDataSourceservice = base.ResolveService<DataSourceService>();
            if (Report.DataSourceRefId != string.Empty)
            {
                dresp = myDataSourceservice.Any(new DataSourceDataRequest444 { RefId = Report.DataSourceRefId, Draw = 1, Start = 0, Length = 100 });
                Report.DataSet = dresp.DataSet;
            }



            Rectangle rec = new Rectangle(Report.Width, Report.Height);
            Report.Doc = new Document(rec);
            Report.Ms1 = new MemoryStream();
            Report.Writer = PdfWriter.GetInstance(Report.Doc, Report.Ms1);
            Report.Writer.Open();
            Report.Doc.Open();
            Report.Writer.PageEvent = new HeaderFooter(Report);
            Report.Writer.CloseStream = true;//important
            Report.Canvas = Report.Writer.DirectContent;
            Report.PageNumber = Report.Writer.PageNumber;
            Report.InitializeSummaryFields();
            GetWatermarkImages();
            //iTextSharp.text.Font link = FontFactory.GetFont("Arial", 12, iTextSharp.text.Font.UNDERLINE, BaseColor.DarkGray);
            // Anchor anchor = new Anchor("xyz",link);
            //anchor.Reference = "http://eb_roby_dev.localhost:5000/ReportRender?refid=eb_roby_dev-eb_roby_dev-3-1127-1854?tab=" + JsonConvert.SerializeObject(Report.DataRow[Report.SerialNumber - 1]);
            // d.Add(anchor);
            //QR & BAR CODES
            Report.Doc.Add(new Paragraph("Barcode EAN.UCC-13"));
            BarcodeEan codeEAN = new BarcodeEan();
            //codeEAN.Code = "4512345678906";

            //Report.Doc.Add(new Paragraph("default:"));
            //Report.Doc.Add(codeEAN.CreateImageWithBarcode(Report.Canvas, null, null));

            //codeEAN.GuardBars = false;
            //Report.Doc.Add(new Paragraph("without guard bars:"));
            //Report.Doc.Add(codeEAN.CreateImageWithBarcode(Report.Canvas, null, null));

            //codeEAN.Baseline = -1f;
            //codeEAN.GuardBars = true;
            //Report.Doc.Add(new Paragraph("text above:"));
            //Report.Doc.Add(codeEAN.CreateImageWithBarcode(Report.Canvas, null, null));

            codeEAN.Baseline = codeEAN.Size;
            Report.Doc.Add(new Paragraph("qr"));

            //BarcodeQRCode qrcode = new BarcodeQRCode("4512345678906", 1, 1, null);
            //Image qrcodeImage = qrcode.getImage();
            //qrcodeImage.setAbsolutePosition(10, 500);
            //qrcodeImage.scalePercent(200);
            //d1.Add(qrcodeImage);

            QRCodeGenerator qrGenerator = new QRCodeGenerator();
            QRCodeData qrCodeData = qrGenerator.CreateQrCode("4512345678906", QRCodeGenerator.ECCLevel.Q);
            BitmapByteQRCode qrCode = new BitmapByteQRCode(qrCodeData);
            byte[] qrCodeImage = qrCode.GetGraphic(20);
            iTextSharp.text.Image img = iTextSharp.text.Image.GetInstance(qrCodeImage);
            img.ScaleAbsolute(200, 200);
            Report.Doc.Add(img);

            Report.Doc.NewPage();

            Report.DrawReportHeader();
            Report.DrawDetail();
            Report.Doc.Close();
            Report.Ms1.Position = 0;//important
            if (Report.DataSourceRefId != string.Empty)
            {
                Report.DataSet.Tables.Clear();
                Report.DataSet.Dispose();
            }
            return new ReportRenderResponse { StreamWrapper = new MemorystreamWrapper(Report.Ms1) };

        }

        private void GetWatermarkImages()
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
    }

    public partial class HeaderFooter : PdfPageEventHelper
    {
        private EbReport Report { get; set; }
        public override void OnStartPage(PdfWriter writer, Document document)
        {
        }
        public override void OnEndPage(PdfWriter writer, Document d)
        {
            Report.DrawPageHeader();
            Report.DrawPageFooter();
            if (Report.IsLastpage == true) Report.DrawReportFooter();
            Report.DrawWaterMark(Report.PdfReader, d, writer);
        }

        public HeaderFooter(EbReport _c) : base()
        {
            this.Report = _c;
        }
    }
}
