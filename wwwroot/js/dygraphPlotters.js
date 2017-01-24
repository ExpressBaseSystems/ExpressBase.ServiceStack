function getRandomColor() {
    var letters = '0123456789ABCDEF';
    var color = '#';
    for (var i = 0; i < 6; i++) {
        color += letters[Math.floor(Math.random() * 16)];
    }
    return color;
}

Array.prototype.contains = function(obj) {
    var i = this.length;
    while (i--) {
        if (this[i] === obj) {
            return true;
        }
    }
    return false;
}

var colorCollection = [];
var ctxCollection = [];

var dy_plotters = {

    barChartPlotter: function(e) {
        var nVal = e.allSeriesPoints.length;
        var barNo = e.seriesIndex;
        var ctx = e.drawingContext;

        if (!ctxCollection.contains(ctx))
            ctxCollection.push(ctx);
        var Mclr = colorCollection[ctxCollection.indexOf(ctx)];
        var cg = e.color.replace('rgb(', '').slice(0, -1).split(",")[1];
        var cb = e.color.replace('rgb(', '').slice(0, -1).split(",")[2];
        cg = parseInt(cg).toString(16);
        cb = parseInt(cb).toString(16);
        if (cg.length < 2) cg = '0' + cg
        if (cb.length < 2) cb = '0' + cb
        var fclr = '#' + Mclr.slice((e.seriesIndex % 3) + 1, (e.seriesIndex % 3) + 3) + cg + cb;
        ctx.fillStyle = fclr;

        var points = e.points;
        var barr = 2 / 3 * (points[1].canvasx - points[0].canvasx);
        var bar_width = barr / nVal;
        ctx.moveTo(e.points[0].canvasx, e.points[0].canvasy);
        var rgba = e.color.slice(0, 3) + 'a' + e.color.slice(3);
        for (var i = 1; i < e.points.length; i++) {
            var p = e.points[i];
            ctx.moveTo(p.canvasx, 0)
            ctx.fillRect(p.canvasx - (barr / 2) + (barNo * bar_width), p.canvasy, bar_width, 1000000000 + p.canvasy);
            ctx.strokeStyle = "#000000";
            ctx.strokeRect(p.canvasx - (barr / 2) + (barNo * bar_width), p.canvasy, bar_width, 1000000000 + p.canvasy);
        }
        
    },

    lineChartPlotter: function (e) {
        var ctx = e.drawingContext;
        var points = e.points;
        ctx.beginPath();
        ctx.moveTo(e.points[0].canvasx, e.points[0].canvasy);
        ctx.fillStyle = e.color;
        for (var i = 1; i < e.points.length; i++) {
            var p = e.points[i];
            ctx.lineTo(p.canvasx, p.canvasy);
        }
        ctx.stroke();
    },

    filledAreaChartPlotter: function(e) {
        var ctx = e.drawingContext;
        var points = e.points;
        ctx.beginPath();
        ctx.moveTo(0, 1000000);
        var rgba = e.color.slice(0, 3) + 'a' + e.color.slice(3);
        ctx.fillStyle = rgba.slice(0, -1) + ',0.3' + rgba.slice(-1);
        var p = '';
        for (var i = 1; i < e.points.length; i++) {
            p = e.points[i];
            ctx.lineTo(p.canvasx, p.canvasy);
        }
        ctx.lineTo(p.canvasx, 1000000);
        ctx.fill();
        ctx.stroke();
    },

    splineChartPlotter: function(e) {
        var ctx = e.drawingContext;
        var points = e.points;
        ctx.beginPath();
        ctx.moveTo(e.points[0].canvasx, e.points[0].canvasy);
        ctx.fillStyle = e.color;
        for (var i = 1; i < e.points.length - 1; i++) {
            var p = e.points[i];
            ctx.bezierCurveTo(p.canvasx, p.canvasy, e.points[i + 1].canvasx, p.canvasy, e.points[i + 1].canvasx, e.points[i + 1].canvasy)
        }
        ctx.stroke();
    },
};
