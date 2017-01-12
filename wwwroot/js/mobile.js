$(document).ready(function () {
    $('#mob').keyup(function () {
        $('#mobres').html(phonenumber($('#mob').val()))

    })

    $('#mob').focusout(function () {
        var colval = $('#mob').val();
        
        var DataCollection = { 9: $('#mob').val() };
       
        $.post('http://localhost:53125/register/TableId', { "TableId": 157, "Colvalues": JSON.stringify(DataCollection) },
        function (result) {
            if (result) {
                document.getElementById('div5').innerHTML = "<img src='http://localhost:53125/images/CheckMark-24x32.png' width='32px'/>";
            }
            else {
                document.getElementById('div5').innerHTML = "<img src='http://localhost:53125/images/Error-24x24.png' width='32px'/>";
            }
        });
    })
    function phonenumber(inputtxt) {
        var phoneno = /^\d{10}$/;
        if ((inputtxt.length == 10)) {
            return 'valid phone number';
        }
        else {
            return 'Invalid phone number';
        }
    }

});
