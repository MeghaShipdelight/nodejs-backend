const express = require('express');
var mongoose = require('mongoose');
var router = express.Router();
const fs = require('fs');
var http = require('http');
var ordersV1Model = require('../../app/models/orders');
var ordersV2Model = require('../../app/models/ordersV2');
const moment = require('moment');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const orderDataAsync = [];
var async = require('async');
var csv = require('csv-parser');
const fastcsv = require("fast-csv");
const bodyParser = require('body-parser');
const busboyBodyParser = require('busboy-body-parser');
const includes = require('array-includes');
const mysql = require('mysql2');
var isNumeric = require("isnumeric");
var parseFloat = require("parse-float");
const cron = require('node-cron');
var path = require('path');
var rimraf = require('rimraf');
var trim = require('trim');


router.get('/getReport', (req, res) => {
    res.send('Hello World!')
});

/*
    Using async get Overall performance data using import_date and booking_date  (02-02-2022)
*/
router.get('/getOverallPerformance/:startDate/:endDate', async (req, res) => {
  // console.log("params---",req.params.startDate+' 00:00:00')
  var startDate = trim(req.params.startDate)
  var endDate = trim(req.params.endDate)
  const overall = [];
  try {
    const OrdersV1 = await ordersV1Model.find({
      "status.import_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      }
    });
    console.log("OrdersV1---", OrdersV1.length)
    for await (let data of OrdersV1) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var api_serviceable_list = api_serviceable_list ? api_serviceable_list : null
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'Instashipin V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        var consignorrto = data.consignor.rto ? data.consignor.rto : null;
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }
      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;
        var status_last_attempt_date = data.status.last_attempt_date ? data.status.last_attempt_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_rto_delivery_date = data.status.rto_delivery_date ? data.status.rto_delivery_date : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_rto_intransit_date = data.status.rto_intransit_date ? data.status.rto_intransit_date : null;
        var status_rto_tracking_no = data.status.rto_tracking_no ? data.status.rto_tracking_no : null;
        var status_zone_code = data.status.zone_code ? data.status.zone_code : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        if (status_delivery_date) {
          var delivery_date = moment(status_delivery_date).format('YYYY-MM-DD');
        } else {
          var delivery_date = ''
        }
        if (status_last_attempt_date) {
          var last_attempt_date = moment(status_last_attempt_date).format('YYYY-MM-DD');
        } else {
          var last_attempt_date = '';
        }
        if (status_import_date) {
          var import_date = moment(status_import_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }
        if (status_rto_initiated_date) {
          var initiated_date = moment(status_rto_initiated_date).format('YYYY-MM-DD');
        } else {
          var initiated_date = '';
        }
        if (status_rto_intransit_date) {
          var intransit_date = moment(status_rto_intransit_date).format('YYYY-MM-DD');
        } else {
          var intransit_date = '';
        }
        if (status_rto_delivery_date) {
          var rto_delivery_date = moment(status_rto_delivery_date).format('YYYY-MM-DD');
        } else {
          var rto_delivery_date = '';
        }

        if (status_delivery_date && status_delivery_date != "0000-00-00 00:00:00" && status_delivery_date != "" && status_pickup_date && status_pickup_date != "0000-00-00 00:00:00" && status_pickup_date != "") {
          const date1 = new Date(status_pickup_date);
          const date2 = new Date(status_delivery_date);
          const diffTime = Math.abs(date2 - date1);
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          // console.log(diffTime + " milliseconds");
          var deliverytat = diffDays
          // console.log("deliverytat@@---",deliverytat + " days");
        } else {
          var deliverytat = "";
          // console.log("deliverytat----",deliverytat)
        }
        if (deliverytat <= "6") {
          var tat = 'Day ' + deliverytat
          //  console.log("tat1---",tat)
        } else if (deliverytat > 6 && deliverytat < 16) {
          var tat = '7 To 15 Days';
          //  console.log("tat2---",tat)
        } else if (deliverytat > 15 && deliverytat < 31) {
          var tat = '16 To 30 Days';
          //  console.log("tat3---",tat)
        } else if (deliverytat > 30) {
          var tat = '>30 Days';
          //  console.log("tat4---",tat)
        } else {
          var tat = '';
          // console.log("tat5---",tat)
        }
      }
      if (data.weight_update) {
        var weightupdate_actual_weight = data.weight_update.actual_weight ? data.weight_update.actual_weight : null;
      }
      if (data.package) {
        var package_actual_weight = data.package.actual_weight ? data.package.actual_weight : null;
        var package_item_description = data.package.item_description ? data.package.item_description : null;
        var package_items = data.package.items ? data.package.items : null;
        var package_pieces = data.package.pieces ? data.package.pieces : null;
        var package_product = data.package.product ? data.package.product : null;
        var package_rp = data.package.rp ? data.package.rp : null;
        var package_volumetric_weight = data.package.volumetric_weight ? data.package.volumetric_weight : null;

      }
      if (data.payments) {
        var payments_cod_collect = data.payments.cod_collect ? data.payments.cod_collect : null;
        if (payments_cod_collect) {
          var payments_codcollect_cod_recived = data.payments.cod_collect.cod_recived ? data.payments.cod_collect.cod_recived : null;
        }
        var payments_cod_payment = data.payments.cod_payment ? data.payments.cod_payment : null;
        if (payments_cod_payment) {
          var payments_codpayment_cod_paid = data.payments.cod_payment.cod_paid ? data.payments.cod_payment.cod_paid : null;
          var payments_codpayment_paidbankrefno = data.payments.cod_payment.paid_bank_ref_no ? data.payments.cod_payment.paid_bank_ref_no : null;
          var payments_codpayment_paiddatetime = data.payments.cod_payment.paid_datetime ? data.payments.cod_payment.paid_datetime : null;
          if (payments_codpayment_paiddatetime) {
            var payments_codpayment_paiddate = moment(payments_codpayment_paiddatetime).format('YYYY-MM-DD');
          } else {
            var payments_codpayment_paiddate = '';
          }
        }

      }
      var order_type = "FORWARD";
      //  console.log("package_rp---",package_rp)
      if (package_rp && package_rp == 1) {
        var order_type = 'REVERSE';
        // console.log("order_type---",order_type)
      }
      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var first_ofd_date = sec_ofd_date = third_ofd_date = last_ofd_date = '';
      var i = 0;
      var count = 0;
      if (tracking != '') {
        // console.log("tracking---",tracking)
        var statuscode = '305';
        var no_of_attempt = 0;
        var tracking_counts = [];
        var obj = {}
        tracking.forEach(function (item) {
          obj[item.status_code] ? obj[item.status_code]++ : obj[item.status_code] = 1;
        });
        // console.log("obj----",obj)  
        // console.log("count---",obj[statuscode])
        no_of_attempt = obj[statuscode];
        // console.log("tracking[0]----",tracking[0])
        for await (let track of tracking) {
          // var track_lsp_status_code = track.lsp_status_code ? track.lsp_status_code : null;
          var track_status = track.status ? track.status : null;
          var track_status_code = track.status_code ? track.status_code : null;
          // var track_lsp_status = track.lsp_status ? track.lsp_status : null;
          var track_parent_status_code = track.parent_status_code ? track.parent_status_code : null;
          // var track_source_status_code = track.source_status_code ? track.source_status_code : null;
          var track_remarks = track.remarks ? track.remarks : null;
          var track_updated_date = track.updated_date ? track.updated_date : null;
          var track_location = track.location ? track.location : null;
          // var track_manualentry = track.manualentry ? track.manualentry : null;
          // console.log("track_status_code.length----",track_status_code.length)

          // console.log("track_status_code----",track_status_code)
          if (track_status_code == '305') {
            if (i == 0) {
              var first_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("first_ofd_date----",first_ofd_date)
            } else if (i == 1) {
              var sec_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("sec_ofd_date----",sec_ofd_date)
            } else if (i == 2) {
              var third_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("third_ofd_date----",third_ofd_date)
            }
            var last_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
            i++;
            // console.log("i---",i)
          } else {
            var first_ofd_date = first_ofd_date;
            var sec_ofd_date = sec_ofd_date;
            var third_ofd_date = third_ofd_date;
            var last_ofd_date = last_ofd_date;
          }


        }
        count = tracking.length;
        if (count > 0) {
          count = count - 1;
          // console.log("tracking[count]['remarks']----",tracking[count]['remarks'])
          var status_remark = tracking[count]['remarks'];
        } else {
          var status_remark = 'PENDING PICKUP';
        }
        if (track_status_code == "I002" || track_status_code == "I003" || track_status_code == "I006" || track_status_code == "I007") {
          // latest_status = 'UNDELIVERED';
          ndr_action = tracking[count]['remarks'];
          // console.log("tracking[count]['remarks']@@@---",tracking[count]['remarks'])
          ndr_action_date = moment(tracking[count]['track_updated_date']).format('YYYY-MM-DD');
          // console.log("ndr_action---",ndr_action)
          // console.log("ndr_action_date---",ndr_action_date)
        } else if (status_dispatch_status == "18") {
          ndr_action = 'CL-REATTEMPT';
          ndr_action_date = '';
        } else {
          ndr_action = '';
          ndr_action_date = '';
        }
      }
      var latest_status = '';
      if (status_dispatch_status == '0') {
        latest_status = "PENDING PICKUP";
      } else if (status_dispatch_status == '1') {
        latest_status = "PICKUP DONE";
      } else if (status_dispatch_status == '2') {
        latest_status = "IN-TRANSIT";
      } else if (status_dispatch_status == '4') {
        latest_status = "OUT FOR DELIVERY"
      } else if (status_dispatch_status == '5') {
        latest_status = "DELIVERED"
        // console.log("latest_status---",latest_status)
      } else if (status_dispatch_status == '6') {
        latest_status = "UNDELIVERED"
      } else if (status_dispatch_status == '7') {
        latest_status = "UNDELIVERED"
      } else if (status_dispatch_status == '10') {
        latest_status = "RTO INITIATED"
      } else if (status_dispatch_status == '11') {
        latest_status = "RTO IN-TRANSIT"
      } else if (status_dispatch_status == '12') {
        latest_status = "RTO OUT FOR DELIVERY"
      } else if (status_dispatch_status == '13') {
        latest_status = "RTO DELIVERED"
      } else if (status_dispatch_status == '14') {
        latest_status = "PICKUP CANCEL BY CLIENT"
      } else if (status_dispatch_status == '15') {
        latest_status = "LOST"
      } else if (status_dispatch_status == '16') {
        latest_status = "SHIPMENT DAMAGE"
      } else if (status_dispatch_status == '17') {
        latest_status = "DANGER GOODS"
      } else if (status_dispatch_status == '18') {
        latest_status = "REATTEMPT"
      } else if (status_dispatch_status == '19') {
        latest_status = "RTO"
      } else if (status_dispatch_status == '20') {
        latest_status = "RTO"
      } else if (status_dispatch_status == '21') {
        latest_status = "SELF COLLECT (CS)"
      } else if (status_dispatch_status == '55') {
        latest_status = "RTO UNDELIVERED"
      } else if (status_dispatch_status == '56') {
        latest_status = "REVERSE UNDELIVERED"
      } else if (status_dispatch_status == '1001') {
        latest_status = "UNATTEMPTED"
      } else if (status_dispatch_status == '8') {
        latest_status = "UNDELIVERED"
      } else {
        latest_status = "PENDING PICKUP";
        // status_remark="PENDING PICKUP";
      }



      if (status_pickup_date && latest_status && track_parent_status_code && track_parent_status_code != '14') {
        var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
      } else {
        var pickup_date = '';
      }


      var latest_status_date = track_updated_date;
      var latest_status_code = '99';
      if (track_status_code) {
        var latest_status_code = track_status_code;
      } else {
        var latest_status_code = '';
      }
      var latest_parent_status_code = '0';
      if (track_parent_status_code) {
        latest_parent_status_code = track_parent_status_code;
      } else {
        latest_parent_status_code = '';
      }

      // console.log("track_status_code---",track_status_code)

      var status_remark, last_undel_reason, ndr_action, ndr_action_date = ''
      // if ((track_parent_status_code == "10" || track_parent_status_code == "18")) {
      //   status_remark = track_remarks;
      //   // console.log("status_remark---",status_remark)
      // } else {
      //   status_remark = '';
      // }
      // console.log("track_parent_status_code------",track_parent_status_code)
      if (track_parent_status_code != '' && (track_parent_status_code == "6" || track_parent_status_code == "55")) {
        last_undel_reason = track_remarks;
        // console.log("last_undel_reason---",last_undel_reason)
      } else {
        last_undel_reason = '';
      }

      if (latest_status_code && (latest_status_code == "I002" || latest_status_code == "I003" || latest_status_code == "I004" || latest_status_code == "I005" || latest_status_code == "I006" || latest_status_code == "I007")) {
        latest_status = 'UNDELIVERED';
      }
      if (track_status_code && (track_status_code == '901' || track_status_code == '902') && track_updated_date && track_updated_date != '') {
        var lost_damage_date = moment(track_updated_date).format('YYYY-MM-DD');
        // console.log("lost_damage_date---",lost_damage_date)
      } else {
        var lost_damage_date = "";
        // console.log("lost_damage_date@@---",lost_damage_date)
      }
      const masters = [
        { dispatch_status_code: 99, status: "PENDING PICKUP", remarks: "pending pickup", dispatch_status: 0 },
        { dispatch_status_code: 100, status: "PICKUP DONE", remarks: "pickup done", dispatch_status: 1 },
        { dispatch_status_code: 102, status: "IN-TRANSIT", remarks: "processing at origin hub", dispatch_status: 2 },
        { dispatch_status_code: 305, status: "OUT FOR DELIVERY", remarks: "out for delivery", dispatch_status: 4 },
        { dispatch_status_code: 400, status: "Delivered", remarks: "delivered", dispatch_status: 5 },
        { dispatch_status_code: 401, status: "RTO Delivered", remarks: "rto delivered", dispatch_status: 13 },
        { dispatch_status_code: 500, status: "UNDELIVERED", remarks: "consignee refused", dispatch_status: 6 },
        { dispatch_status_code: 501, status: "UNDELIVERED", remarks: "incomplete address", dispatch_status: 6 },
        { dispatch_status_code: 503, status: "UNDELIVERED", remarks: "oda", dispatch_status: 6 },
        { dispatch_status_code: 504, status: "UNDELIVERED", remarks: "consignee shifted", dispatch_status: 6 },
        { dispatch_status_code: 505, status: "UNDELIVERED", remarks: "DAMAGED", dispatch_status: 6 },
        { dispatch_status_code: 506, status: "UNDELIVERED", remarks: "no such consignee", dispatch_status: 6 },
        { dispatch_status_code: 507, status: "UNDELIVERED", remarks: "future delivery", dispatch_status: 6 },
        { dispatch_status_code: 508, status: "UNDELIVERED", remarks: "cod not ready", dispatch_status: 6 },
        { dispatch_status_code: 509, status: "UNDELIVERED", remarks: "residence/office closed", dispatch_status: 6 },
        { dispatch_status_code: 510, status: "UNDELIVERED", remarks: "out of station", dispatch_status: 6 },
        { dispatch_status_code: 511, status: "UNDELIVERED", remarks: "shipment lost", dispatch_status: 6 },
        { dispatch_status_code: 512, status: "UNDELIVERED", remarks: "Dangerous Goods", dispatch_status: 6 },
        { dispatch_status_code: 513, status: "UNDELIVERED", remarks: "Self Collect", dispatch_status: 6 },
        { dispatch_status_code: 514, status: "UNDELIVERED", remarks: "Held With Govt Authority", dispatch_status: 6 },
        { dispatch_status_code: 515, status: "UNDELIVERED", remarks: "consignee not available", dispatch_status: 6 },
        { dispatch_status_code: 516, status: "UNDELIVERED", remarks: "consignee not responding", dispatch_status: 6 },
        { dispatch_status_code: 517, status: "UNDELIVERED", remarks: "misroute", dispatch_status: 6 },
        { dispatch_status_code: 518, status: "UNDELIVERED", remarks: "on hold", dispatch_status: 6 },
        { dispatch_status_code: 519, status: "UNDELIVERED", remarks: "restricted area", dispatch_status: 6 },
        { dispatch_status_code: 520, status: "UNDELIVERED", remarks: "snatched by consignee", dispatch_status: 6 },
        { dispatch_status_code: 521, status: "UNDELIVERED", remarks: "disturbance/natural disaster/strike/COVID", dispatch_status: 6 },
        { dispatch_status_code: 522, status: "UNDELIVERED", remarks: "Open Delivery", dispatch_status: 6 },
        { dispatch_status_code: 523, status: "UNDELIVERED", remarks: "Customer denied - OTP Delivery", dispatch_status: 6 },
        { dispatch_status_code: 524, status: "UNDELIVERED", remarks: "Time Constraint / Dispute", dispatch_status: 6 },
        { dispatch_status_code: 600, status: "RTO INITIATED", remarks: "rto initiated ", dispatch_status: 10 },
        { dispatch_status_code: 601, status: "RTO IN-TRANSIT", remarks: "rto intransit ", dispatch_status: 11 },
        { dispatch_status_code: 615, status: "RTO UNDELIVERED ", remarks: "Vendor refused ", dispatch_status: 55 },
        { dispatch_status_code: 900, status: "Pickup Cancelled ", remarks: "Pickup Cancelled by Client ", dispatch_status: 14 },
        { dispatch_status_code: 901, status: "LOST ", remarks: "SHIPMENT LOST ", dispatch_status: 15 },
        { dispatch_status_code: 902, status: "SHIPMENT DAMAGE ", remarks: "SHIPMENT DAMAGE ", dispatch_status: 16 },
        { dispatch_status_code: 951, status: "REATTEMPT ", remarks: "Reattempt ", dispatch_status: 18 },
        { dispatch_status_code: 1001, status: "Unattempted ", remarks: "Unattempted ", dispatch_status: 1001 },
        { dispatch_status_code: "I001", status: "SWC ", remarks: "Shared with Client ", dispatch_status: 7 },
        { dispatch_status_code: "I002", status: "CL-REATTEMPT ", remarks: "Client Reattempt ", dispatch_status: 7 },
        { dispatch_status_code: "I003", status: "CL-RTO-INITIATED ", remarks: "cl rto initiated ", dispatch_status: 7 },
        { dispatch_status_code: "I004", status: "CL-HOLD ", remarks: "Client Hold ", dispatch_status: 7 },
        { dispatch_status_code: "I005", status: "CL-SELFCOLLECT ", remarks: "Client Self Collect ", dispatch_status: 7 },
        { dispatch_status_code: "I006", status: "SD-REATTEMPT ", remarks: "Shipdelight Reattempt ", dispatch_status: 7 },
        { dispatch_status_code: "I007", status: "SD-RTO-INITIATED ", remarks: "sd rto initiated ", dispatch_status: 7 },
        { dispatch_status_code: "I008", status: "SD-SELFCOLLECT ", remarks: "Shipdelight Self Collect ", dispatch_status: 7 },
        { dispatch_status_code: "N001", status: "Whatsapp Calling ", remarks: "Whatsapp Calling ", dispatch_status: 8 },
        { dispatch_status_code: 402, status: "Partial Delivered ", remarks: "Partial Delivered ", dispatch_status: 21 },
        { dispatch_status_code: "R1207", status: "Reverse UNDELIVERED ", remarks: "on hold ", dispatch_status: 6 },
        { dispatch_status_code: "R1208", status: "Reverse UNDELIVERED ", remarks: "vendor not available ", dispatch_status: 6 },
      ]

      if (track_parent_status_code != '' && track_parent_status_code == '6') {
        // console.log("track_parent_status_code---",track_parent_status_code)
        var result = masters.find(c => c.dispatch_status == status_dispatch_status && c.dispatch_status_code == status_dispatch_status_code)
        if (result != undefined) {
          // console.log("result---",result.remarks)
          var latest_undelivered_status_remark = result.remarks;
          // console.log("latest_undelivered_status_remark---",latest_undelivered_status_remark)
        } else {
          var latest_undelivered_status_remark = '';
        }

      } else {
        latest_undelivered_status_remark = '';
      }


      await overall.push({
        sync_source: sync_source,
        consignorname: consignorname,
        consignorsub_vendorname: consignorsub_vendorname,
        orderno: orderno,
        airwaybilno: airwaybilno,
        status_rto_tracking_no: status_rto_tracking_no,
        import_date: import_date,
        pickup_date: pickup_date,
        courier_code: courier_code,
        track_location: track_location,
        latest_status: latest_status,
        last_undel_reason: last_undel_reason,
        ndr_action: ndr_action,
        ndr_action_date: ndr_action_date,
        latest_undelivered_status_remark: latest_undelivered_status_remark,
        status_remark: status_remark,
        consigneefirstname: consigneefirstname,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneestate: consigneestate,
        consigneepincode: consigneepincode,
        consigneetelephone1: consigneetelephone1,
        payment_paytype: package_product,
        packages_description: package_item_description,
        packages_quantity: package_pieces,
        packages_actual_weight: package_actual_weight,
        packages_volumetric_weight: package_volumetric_weight,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorcity: consignorsub_vendorcity,
        lost_damage_date: lost_damage_date,
        delivery_date: delivery_date,
        initiated_date: initiated_date,
        intransit_date: intransit_date,
        rto_delivery_date: rto_delivery_date,
        no_of_attempt: no_of_attempt,
        last_attempt_date: last_attempt_date,
        delivery_tat: deliverytat,
        payment_codpayment_paiddate: payments_codpayment_paiddate,
        payment_codpayment_cod_paid: payments_codpayment_cod_paid,
        payment_codpayment_paidbankrefno: payments_codpayment_paidbankrefno,
        payment_codcollect_cod_recived: payments_codcollect_cod_recived,
        status_zone_code: status_zone_code,
        first_ofd_date: first_ofd_date,
        sec_ofd_date: sec_ofd_date,
        third_ofd_date: third_ofd_date,
        last_ofd_date: last_ofd_date,
        weightupdate_actual_weight: weightupdate_actual_weight,
        order_type: order_type,

      })

    }

    const OrdersV2 = await ordersV2Model.find({
      "status.booking_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      }
    });
    console.log("OrdersV2---", OrdersV2.length)

    for await (let data of OrdersV2) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var api_serviceable_list = api_serviceable_list ? api_serviceable_list : null
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        var consignorrto = data.consignor.rto ? data.consignor.rto : null;
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }
      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;
        var status_last_attempt_date = data.status.last_attempt_date ? data.status.last_attempt_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_rto_delivery_date = data.status.rto_delivery_date ? data.status.rto_delivery_date : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_rto_intransit_date = data.status.rto_intransit_date ? data.status.rto_intransit_date : null;
        var status_rto_tracking_no = data.status.rto_tracking_no ? data.status.rto_tracking_no : null;
        var status_zone_code = data.status.zone_code ? data.status.zone_code : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        if (status_delivery_date) {
          var delivery_date = moment(status_delivery_date).format('YYYY-MM-DD');
        } else {
          var delivery_date = ''
        }
        if (status_last_attempt_date) {
          var last_attempt_date = moment(status_last_attempt_date).format('YYYY-MM-DD');
        } else {
          var last_attempt_date = '';
        }
        if (status_booking_date) {
          var import_date = moment(status_booking_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }
        if (status_rto_initiated_date) {
          var initiated_date = moment(status_rto_initiated_date).format('YYYY-MM-DD');
        } else {
          var initiated_date = '';
        }
        if (status_rto_intransit_date) {
          var intransit_date = moment(status_rto_intransit_date).format('YYYY-MM-DD');
        } else {
          var intransit_date = '';
        }
        if (status_rto_delivery_date) {
          var rto_delivery_date = moment(status_rto_delivery_date).format('YYYY-MM-DD');
        } else {
          var rto_delivery_date = '';
        }

        if (status_delivery_date && status_delivery_date != "0000-00-00 00:00:00" && status_delivery_date != "" && status_pickup_date && status_pickup_date != "0000-00-00 00:00:00" && status_pickup_date != "") {
          const date1 = new Date(status_pickup_date);
          const date2 = new Date(status_delivery_date);
          const diffTime = Math.abs(date2 - date1);
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          // console.log(diffTime + " milliseconds");
          var deliverytat = diffDays
          // console.log("deliverytat@@---",deliverytat + " days");
        } else {
          var deliverytat = "";
          // console.log("deliverytat----",deliverytat)
        }
        if (deliverytat <= "6") {
          var tat = 'Day ' + deliverytat
          //  console.log("tat1---",tat)
        } else if (deliverytat > 6 && deliverytat < 16) {
          var tat = '7 To 15 Days';
          //  console.log("tat2---",tat)
        } else if (deliverytat > 15 && deliverytat < 31) {
          var tat = '16 To 30 Days';
          //  console.log("tat3---",tat)
        } else if (deliverytat > 30) {
          var tat = '>30 Days';
          //  console.log("tat4---",tat)
        } else {
          var tat = '';
          // console.log("tat5---",tat)
        }

      }
      if (data.weight_update) {
        var weightupdate_actual_weight = data.weight_update.actual_weight ? data.weight_update.actual_weight : null;
      }

      var packages = data.packages ? data.packages : [];
      if (packages != '') {
        for (let pack of packages) {
          var packages_description = pack.description ? pack.description : null;
          // var packages_sku = pack.sku ? pack.sku : null;
          var packages_quantity = pack.quantity ? pack.quantity : null;
          var packages_actual_weight = pack.actual_weight ? pack.actual_weight : null;
          var package_volumetric_weight = pack.volumetric_weight ? pack.volumetric_weight : null;
          // var packages_length = pack.length ? pack.length : null;
          // var packages_breadth = pack.breadth ? pack.breadth : null;
          // var packages_height = pack.height ? pack.height : null;
          var packages_price = pack.price ? pack.price : null;

          var total_quantity = 0, total_weight = 0, total_price = 0;
          // console.log("packages_quantity---",packages_quantity)
          total_price += (packages_price != '' ? parseFloat(packages_price) : 0);//product_value
          total_quantity += !isNumeric(packages_quantity) ? 1 : packages_quantity;
          total_weight += !isNumeric(packages_actual_weight) ? 0 : packages_actual_weight;
          // console.log("total_price---",total_price)
        }

      }
      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)

      var first_ofd_date = sec_ofd_date = third_ofd_date = last_ofd_date = '';
      var i = 0;
      var count = 0;
      if (tracking != '') {
        // console.log("tracking---",tracking)
        var statuscode = '305';
        var no_of_attempt = 0;
        var tracking_counts = [];
        var obj = {}
        tracking.forEach(function (item) {
          obj[item.status_code] ? obj[item.status_code]++ : obj[item.status_code] = 1;
        });
        // console.log("obj----",obj)  
        // console.log("count---",obj[statuscode])
        no_of_attempt = obj[statuscode];
        // console.log("tracking[0]----",tracking[0])
        for await (let track of tracking) {
          // var track_lsp_status_code = track.lsp_status_code ? track.lsp_status_code : null;
          var track_status = track.status ? track.status : 'PENDING PICKUP';
          var track_status_code = track.status_code ? track.status_code : null;
          // var track_lsp_status = track.lsp_status ? track.lsp_status : null;
          var track_parent_status_code = track.parent_status_code ? track.parent_status_code : null;
          // var track_source_status_code = track.source_status_code ? track.source_status_code : null;
          var track_remarks = track.remarks ? track.remarks : null;
          var track_updated_date = track.updated_date ? track.updated_date : null;
          var track_location = track.location ? track.location : null;
          // var track_manualentry = track.manualentry ? track.manualentry : null;
          // console.log("track_status_code.length----",track_status_code.length)



          // console.log("track_status_code--",track_status_code)
          if (track_status_code == '305') {
            // console.log("reached hear")
            if (i == 0) {
              var first_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("first_ofd_date----",first_ofd_date)
            }
            if (i == 1) {
              var sec_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("sec_ofd_date----",sec_ofd_date)
            }
            if (i == 2) {
              var third_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("third_ofd_date----",third_ofd_date)
            }
            var last_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
            i++;
            // console.log("i---",i)
          }
          else {
            var first_ofd_date = first_ofd_date;
            var sec_ofd_date = sec_ofd_date;
            var third_ofd_date = third_ofd_date;
            var last_ofd_date = last_ofd_date;
          }

        }

        count = tracking.length;
        if (count > 0) {
          count = count - 1;
          var status_remark = tracking[count]['remarks'];
        } else {
          var status_remark = 'PENDING PICKUP';
        }

        if (track_status_code != '' && (track_status_code == "I002" || track_status_code == "I003" || track_status_code == "I004" || track_status_code == "I005" || track_status_code == "I006" || track_status_code == "I007")) {
          // latest_status = 'UNDELIVERED';
          ndr_action = tracking[count]['remarks'];
          ndr_action_date = moment(tracking[count]['track_updated_date']).format('YYYY-MM-DD');
          // console.log("ndr_action---",ndr_action)
          // console.log("ndr_action_date---",ndr_action_date)
        } else if (status_dispatch_status == "18") {
          ndr_action = 'CL-REATTEMPT';
          ndr_action_date = '';
        } else {
          ndr_action = '';
          ndr_action_date = '';
        }

      }
      var latest_status = '';
      if (status_dispatch_status == '0') {
        latest_status = "PENDING PICKUP";
      } else if (status_dispatch_status == '1') {
        latest_status = "PICKUP DONE";
      } else if (status_dispatch_status == '2') {
        latest_status = "IN-TRANSIT";
      } else if (status_dispatch_status == '4') {
        latest_status = "OUT FOR DELIVERY"
      } else if (status_dispatch_status == '5') {
        latest_status = "DELIVERED"
      } else if (status_dispatch_status == '6') {
        latest_status = "UNDELIVERED"
      } else if (status_dispatch_status == '7') {
        latest_status = "UNDELIVERED"
      } else if (status_dispatch_status == '10') {
        latest_status = "RTO INITIATED"
      } else if (status_dispatch_status == '11') {
        latest_status = "RTO IN-TRANSIT"
      } else if (status_dispatch_status == '12') {
        latest_status = "RTO OUT FOR DELIVERY"
      } else if (status_dispatch_status == '13') {
        latest_status = "RTO DELIVERED"
      } else if (status_dispatch_status == '14') {
        latest_status = "PICKUP CANCEL BY CLIENT"
      } else if (status_dispatch_status == '15') {
        latest_status = "LOST"
      } else if (status_dispatch_status == '16') {
        latest_status = "SHIPMENT DAMAGE"
      } else if (status_dispatch_status == '17') {
        latest_status = "DANGER GOODS"
      } else if (status_dispatch_status == '18') {
        latest_status = "REATTEMPT"
      } else if (status_dispatch_status == '19') {
        latest_status = "RTO"
      } else if (status_dispatch_status == '20') {
        latest_status = "RTO"
      } else if (status_dispatch_status == '21') {
        latest_status = "SELF COLLECT (CS)"
      } else if (status_dispatch_status == '55') {
        latest_status = "RTO UNDELIVERED"
      } else if (status_dispatch_status == '56') {
        latest_status = "REVERSE UNDELIVERED"
      } else if (status_dispatch_status == '1001') {
        latest_status = "UNATTEMPTED"
      } else if (status_dispatch_status == '8') {
        latest_status = "UNDELIVERED"
      } else {
        latest_status = "PENDING PICKUP";
        // status_remark="PENDING PICKUP";
      }


      // console.log("status_remark---",status_remark)

      var latest_status_date = track_updated_date;
      var latest_status_code = '99';
      if (track_status_code) {
        var latest_status_code = track_status_code;
      } else {
        var latest_status_code = '';
      }
      var latest_parent_status_code = '0';
      if (track_parent_status_code) {
        latest_parent_status_code = track_parent_status_code;
      } else {
        latest_parent_status_code = '';
      }

      // console.log("track_status_code---",track_status_code)


      // console.log("track_parent_status_code---",track_parent_status_code)
      var status_remark, last_undel_reason, ndr_action, ndr_action_date = ''
      // console.log("track_parent_status_code---",track_parent_status_code)
      // if ((track_parent_status_code == "10" || track_parent_status_code == "18")) {
      //   status_remark = track_remarks;
      //   // console.log("status_remark---",status_remark)
      // } else {
      //   status_remark = '';
      // }
      // console.log("track_parent_status_code---",track_parent_status_code)
      if (track_parent_status_code != '' && (track_parent_status_code == "6" || track_parent_status_code == "55")) {
        last_undel_reason = track_remarks;
        // console.log("last_undel_reason---",last_undel_reason)
      } else {
        last_undel_reason = '';
      }

      // console.log("latest_status_code---",latest_status_code)
      if (latest_status_code && (latest_status_code == "I002" || latest_status_code == "I003" || latest_status_code == "I004" || latest_status_code == "I005" || latest_status_code == "I006" || latest_status_code == "I007")) {
        latest_status = 'UNDELIVERED';
      }


      // console.log("track_parent_status_code---",track_parent_status_code)
      if (status_pickup_date && latest_status && track_parent_status_code && track_parent_status_code != '14') {
        var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
      } else {
        var pickup_date = '';
      }
      var lost_damage_date = ''
      if (track_status_code && (track_status_code == '901' || track_status_code == '902') && track_updated_date && track_updated_date != '') {
        lost_damage_date = moment(track_updated_date).format('YYYY-MM-DD');
        // console.log("lost_damage_date---",lost_damage_date)
      } else {
        lost_damage_date = "";
        // console.log("lost_damage_date@@---",lost_damage_date)
      }

      var order_type = "FORWARD";
      //  console.log("package_rp---",package_rp)
      // if (package_rp && package_rp == 1) {
      //   var order_type = 'REVERSE';
      //   // console.log("order_type---",order_type)
      // }

      const masters = [
        { dispatch_status_code: 99, status: "PENDING PICKUP", remarks: "pending pickup", dispatch_status: 0 },
        { dispatch_status_code: 100, status: "PICKUP DONE", remarks: "pickup done", dispatch_status: 1 },
        { dispatch_status_code: 102, status: "IN-TRANSIT", remarks: "processing at origin hub", dispatch_status: 2 },
        { dispatch_status_code: 305, status: "OUT FOR DELIVERY", remarks: "out for delivery", dispatch_status: 4 },
        { dispatch_status_code: 400, status: "Delivered", remarks: "delivered", dispatch_status: 5 },
        { dispatch_status_code: 401, status: "RTO Delivered", remarks: "rto delivered", dispatch_status: 13 },
        { dispatch_status_code: 500, status: "UNDELIVERED", remarks: "consignee refused", dispatch_status: 6 },
        { dispatch_status_code: 501, status: "UNDELIVERED", remarks: "incomplete address", dispatch_status: 6 },
        { dispatch_status_code: 503, status: "UNDELIVERED", remarks: "oda", dispatch_status: 6 },
        { dispatch_status_code: 504, status: "UNDELIVERED", remarks: "consignee shifted", dispatch_status: 6 },
        { dispatch_status_code: 505, status: "UNDELIVERED", remarks: "DAMAGED", dispatch_status: 6 },
        { dispatch_status_code: 506, status: "UNDELIVERED", remarks: "no such consignee", dispatch_status: 6 },
        { dispatch_status_code: 507, status: "UNDELIVERED", remarks: "future delivery", dispatch_status: 6 },
        { dispatch_status_code: 508, status: "UNDELIVERED", remarks: "cod not ready", dispatch_status: 6 },
        { dispatch_status_code: 509, status: "UNDELIVERED", remarks: "residence/office closed", dispatch_status: 6 },
        { dispatch_status_code: 510, status: "UNDELIVERED", remarks: "out of station", dispatch_status: 6 },
        { dispatch_status_code: 511, status: "UNDELIVERED", remarks: "shipment lost", dispatch_status: 6 },
        { dispatch_status_code: 512, status: "UNDELIVERED", remarks: "Dangerous Goods", dispatch_status: 6 },
        { dispatch_status_code: 513, status: "UNDELIVERED", remarks: "Self Collect", dispatch_status: 6 },
        { dispatch_status_code: 514, status: "UNDELIVERED", remarks: "Held With Govt Authority", dispatch_status: 6 },
        { dispatch_status_code: 515, status: "UNDELIVERED", remarks: "consignee not available", dispatch_status: 6 },
        { dispatch_status_code: 516, status: "UNDELIVERED", remarks: "consignee not responding", dispatch_status: 6 },
        { dispatch_status_code: 517, status: "UNDELIVERED", remarks: "misroute", dispatch_status: 6 },
        { dispatch_status_code: 518, status: "UNDELIVERED", remarks: "on hold", dispatch_status: 6 },
        { dispatch_status_code: 519, status: "UNDELIVERED", remarks: "restricted area", dispatch_status: 6 },
        { dispatch_status_code: 520, status: "UNDELIVERED", remarks: "snatched by consignee", dispatch_status: 6 },
        { dispatch_status_code: 521, status: "UNDELIVERED", remarks: "disturbance/natural disaster/strike/COVID", dispatch_status: 6 },
        { dispatch_status_code: 522, status: "UNDELIVERED", remarks: "Open Delivery", dispatch_status: 6 },
        { dispatch_status_code: 523, status: "UNDELIVERED", remarks: "Customer denied - OTP Delivery", dispatch_status: 6 },
        { dispatch_status_code: 524, status: "UNDELIVERED", remarks: "Time Constraint / Dispute", dispatch_status: 6 },
        { dispatch_status_code: 600, status: "RTO INITIATED", remarks: "rto initiated ", dispatch_status: 10 },
        { dispatch_status_code: 601, status: "RTO IN-TRANSIT", remarks: "rto intransit ", dispatch_status: 11 },
        { dispatch_status_code: 615, status: "RTO UNDELIVERED ", remarks: "Vendor refused ", dispatch_status: 55 },
        { dispatch_status_code: 900, status: "Pickup Cancelled ", remarks: "Pickup Cancelled by Client ", dispatch_status: 14 },
        { dispatch_status_code: 901, status: "LOST ", remarks: "SHIPMENT LOST ", dispatch_status: 15 },
        { dispatch_status_code: 902, status: "SHIPMENT DAMAGE ", remarks: "SHIPMENT DAMAGE ", dispatch_status: 16 },
        { dispatch_status_code: 951, status: "REATTEMPT ", remarks: "Reattempt ", dispatch_status: 18 },
        { dispatch_status_code: 1001, status: "Unattempted ", remarks: "Unattempted ", dispatch_status: 1001 },
        { dispatch_status_code: "I001", status: "SWC ", remarks: "Shared with Client ", dispatch_status: 7 },
        { dispatch_status_code: "I002", status: "CL-REATTEMPT ", remarks: "Client Reattempt ", dispatch_status: 7 },
        { dispatch_status_code: "I003", status: "CL-RTO-INITIATED ", remarks: "cl rto initiated ", dispatch_status: 7 },
        { dispatch_status_code: "I004", status: "CL-HOLD ", remarks: "Client Hold ", dispatch_status: 7 },
        { dispatch_status_code: "I005", status: "CL-SELFCOLLECT ", remarks: "Client Self Collect ", dispatch_status: 7 },
        { dispatch_status_code: "I006", status: "SD-REATTEMPT ", remarks: "Shipdelight Reattempt ", dispatch_status: 7 },
        { dispatch_status_code: "I007", status: "SD-RTO-INITIATED ", remarks: "sd rto initiated ", dispatch_status: 7 },
        { dispatch_status_code: "I008", status: "SD-SELFCOLLECT ", remarks: "Shipdelight Self Collect ", dispatch_status: 7 },
        { dispatch_status_code: "N001", status: "Whatsapp Calling ", remarks: "Whatsapp Calling ", dispatch_status: 8 },
        { dispatch_status_code: 402, status: "Partial Delivered ", remarks: "Partial Delivered ", dispatch_status: 21 },
        { dispatch_status_code: "R1207", status: "Reverse UNDELIVERED ", remarks: "on hold ", dispatch_status: 6 },
        { dispatch_status_code: "R1208", status: "Reverse UNDELIVERED ", remarks: "vendor not available ", dispatch_status: 6 },
      ]
      // console.log("track_parent_status_code---",track_parent_status_code)
      var latest_undelivered_status_remark = '';
      if (track_parent_status_code != '' && track_parent_status_code == '6') {
        var result = masters.find(c => c.dispatch_status == status_dispatch_status && c.dispatch_status_code == status_dispatch_status_code)
        if (result != undefined) {
          // console.log("result---",result.remarks)
          latest_undelivered_status_remark = result.remarks;
          // console.log("latest_undelivered_status_remark---",latest_undelivered_status_remark)
        } else {
          latest_undelivered_status_remark = '';
          // console.log("latest_undelivered_status_remark@@---",latest_undelivered_status_remark)
        }

      } else {
        latest_undelivered_status_remark = '';
      }
      await overall.push({
        sync_source: sync_source,
        consignorname: consignorname,
        consignorsub_vendorname: consignorsub_vendorname,
        orderno: orderno,
        airwaybilno: airwaybilno,
        status_rto_tracking_no: status_rto_tracking_no,
        import_date: import_date,
        pickup_date: pickup_date,
        courier_code: courier_code,
        track_location: track_location,
        latest_status: latest_status,
        last_undel_reason: last_undel_reason,
        ndr_action: ndr_action,
        ndr_action_date: ndr_action_date,
        latest_undelivered_status_remark: latest_undelivered_status_remark,
        status_remark: status_remark,
        consigneefirstname: consigneefirstname,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneestate: consigneestate,
        consigneepincode: consigneepincode,
        consigneetelephone1: consigneetelephone1,
        payment_paytype: payment_paytype,
        packages_description: packages_description,
        packages_quantity: total_quantity,
        packages_actual_weight: total_weight,
        packages_volumetric_weight: package_volumetric_weight,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorcity: consignorsub_vendorcity,
        lost_damage_date: lost_damage_date,
        delivery_date: delivery_date,
        initiated_date: initiated_date,
        intransit_date: intransit_date,
        rto_delivery_date: rto_delivery_date,
        no_of_attempt: no_of_attempt,
        last_attempt_date: last_attempt_date,
        delivery_tat: deliverytat,
        payment_codpayment_paiddate: payments_codpayment_paiddate,
        payment_codpayment_cod_paid: payments_codpayment_cod_paid,
        payment_codpayment_paidbankrefno: payments_codpayment_paidbankrefno,
        payment_codcollect_cod_recived: payments_codcollect_cod_recived,
        status_zone_code: status_zone_code,
        first_ofd_date: first_ofd_date,
        sec_ofd_date: sec_ofd_date,
        third_ofd_date: third_ofd_date,
        last_ofd_date: last_ofd_date,
        weightupdate_actual_weight: weightupdate_actual_weight,
        order_type: order_type,

      })
    }


    var date = moment(new Date()).format('yyyy-MM-DD');
    var hostname = req.headers.host;
    var currentPath = process.cwd();
    var dir = currentPath + '/src/public/OverAll_Performance_Reports/';
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }
    var fileName = 'OverAll_Performance_Report_' + startDate + '_' + endDate + '.csv';

    const csvWriter = createCsvWriter({

      // Output csv file name is geek_data
      path: dir + fileName,
      options:
        { flags: 'a' },
      header: [

        // Title of the columns (column_names)
        { id: 'sync_source', title: 'Data Source' },
        { id: 'consignorname', title: 'Client Name' },
        { id: 'consignorsub_vendorname', title: 'Sub Vendor Name' },
        { id: 'orderno', title: 'Order No' },
        { id: 'airwaybilno', title: 'AirwayBill No' },
        { id: 'status_rto_tracking_no', title: 'RTO No' },
        { id: 'import_date', title: 'Import Date' },
        { id: 'pickup_date', title: 'Pickup Date' },
        { id: 'courier_code', title: 'Courier Name' },
        { id: 'track_location', title: 'Location' },
        { id: 'latest_status', title: 'Latest Status' },
        { id: 'last_undel_reason', title: 'Status Remark' },
        { id: 'ndr_action', title: 'NDR Action' },
        { id: 'latest_undelivered_status_remark', title: 'Last Undelivered Reason' },
        { id: 'status_remark', title: 'Last Courier Reason' },
        { id: 'consigneefirstname', title: 'Consignee Firstname' },
        { id: 'consigneeaddress1', title: 'Consignee Address1' },
        { id: 'consigneeaddress2', title: 'Consignee Address2' },
        { id: 'consigneecity', title: 'Destination City' },
        { id: 'consigneestate', title: 'State' },
        { id: 'consigneepincode', title: 'Pincode' },
        { id: 'consigneetelephone1', title: 'Consignee Phone1' },
        { id: 'payment_paytype', title: 'Pay Type' },
        { id: 'packages_description', title: 'Package Title' },
        // { id: 'packages_price', title: 'Price' },
        { id: 'packages_quantity', title: 'Quantity' },
        { id: 'packages_actual_weight', title: 'Actual Weight' },
        { id: 'packages_volumetric_weight', title: 'Volumetric Weight' },
        { id: 'consignorsub_vendorphone', title: 'Sub Vendor Contact' },
        { id: 'consignorsub_vendorcity', title: 'Sub Vendor Pickup City' },
        { id: 'consignorsub_vendorpincode', title: 'Sub Vendor Pincode' },
        { id: 'lost_damage_date', title: 'Lost/Damage Date' },
        { id: 'delivery_date', title: 'Delivery Date' },
        { id: 'ndr_action_date', title: 'Action Date' },
        { id: 'initiated_date', title: 'RTO Initiated Date' },
        { id: 'intransit_date', title: 'RTO_Intransit Date' },
        { id: 'rto_delivery_date', title: 'RTO Delivery Date' },
        { id: 'no_of_attempt', title: 'No Of Attempt' },
        { id: 'last_attempt_date', title: 'Last Scan Date' },
        { id: 'delivery_tat', title: 'Delivery TAT' },
        { id: 'payment_codpayment_paiddate', title: 'COD Payment Paid Date' },
        { id: 'payment_codpayment_cod_paid', title: 'Amount Paid' },
        { id: 'payment_codpayment_paidbankrefno', title: 'Client Bank Ref No' },
        { id: 'payment_codcollect_cod_recived', title: 'Recived Paid' },
        { id: 'status_zone_code', title: 'Zone' },
        { id: 'first_ofd_date', title: '1st OFD Date' },
        { id: 'sec_ofd_date', title: '2nd OFD Date' },
        { id: 'third_ofd_date', title: '3rd OFD Date' },
        { id: 'last_ofd_date', title: 'Last OFD Date' },
        { id: 'weightupdate_actual_weight', title: 'Updated Weight' },
        { id: 'order_type', title: 'Order type' },

      ]
    });
    csvWriter
      .writeRecords(overall)
      .then(() => console.log('Data uploaded into csv successfully'));
    res.json({
      status: 200,
      data: 'http://' + hostname + '/OverAll_Performance_Reports/' + fileName
    })

  } catch (error) {
    console.log(`Error: ${error.message}`)
    return res.status(400).json({
      message: 'Bad Request',
    })
  }

})

/*
    Get pending pickup data using date range and status.dispatch_status = 0 , status.dispatch_status_code = 99
*/
router.get('/getPendingPickups/:startDate/:endDate', async (req, res) => {
  var startDate = trim(req.params.startDate)
  var endDate = trim(req.params.endDate)
  const pendingPickup = [];
  // console.log("params---",req.params)
  try {
    const OrdersV1 = await ordersV1Model.find({
      "status.import_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      "status.dispatch_status": '0',
    });
    console.log("OrdersV1-----", OrdersV1.length)
    for await (let data of OrdersV1) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'Instashipin V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        var consignorrto = data.consignor.rto ? data.consignor.rto : null;
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        if (status_import_date) {
          var import_date = moment(status_import_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }

        const date1 = new Date();
        // console.log("date1----",date1)
        const date2 = new Date(status_import_date);
        const diffTime = Math.abs(date2 - date1);
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        // console.log(diffTime + " milliseconds");
        var deliverytat = diffDays
        // console.log("deliverytat@@---",deliverytat);

        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
          //  console.log("tat1---",tat)
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
          //  console.log("tat2---",tat)
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
          //  console.log("tat3---",tat)
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
          //  console.log("tat4---",tat)
        } else {
          var aging = '';
          // console.log("tat5---",tat)
        }

      }
      if (data.package) {
        var package_actual_weight = data.package.actual_weight ? data.package.actual_weight : null;
        var package_item_description = data.package.item_description ? data.package.item_description : null;
        var package_items = data.package.items ? data.package.items : null;
        var package_pieces = data.package.pieces ? data.package.pieces : null;
        var package_product = data.package.product ? data.package.product : null;
        var package_product_value = data.package.product_value ? data.package.product_value : null;
        var package_rp = data.package.rp ? data.package.rp : null;

      }


      var order_type = "FORWARD";
      //  console.log("package_rp---",package_rp)
      if (package_rp && package_rp == 1) {
        var order_type = 'REVERSE';
        // console.log("order_type---",order_type)
      }

      await pendingPickup.push({
        airwaybilno: airwaybilno,
        sync_source: sync_source,
        import_date: import_date,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        packages_actual_weight: package_actual_weight,
        packages_description: package_item_description,
        packages_quantity: package_pieces,
        packages_product_value: package_product_value,
        payment_paytype: package_product,
        order_type: order_type,
        delivery_tat: deliverytat,
        aging: aging,
        latest_undelivered_status_remark: "PENDING PICKUP",
      })
    }
    const OrdersV2 = await ordersV2Model.find({
      "status.booking_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      "status.dispatch_status": '0',
    })

    console.log("OrdersV2---", OrdersV2.length)
    for await (let data of OrdersV2) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        var consignorrto = data.consignor.rto ? data.consignor.rto : null;

        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_rp = data.status.rp ? data.status.rp : null;
        if (status_booking_date) {
          var import_date = moment(status_booking_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }

        const date1 = new Date();
        // console.log("date1----",date1)
        const date2 = new Date(status_booking_date);
        const diffTime = Math.abs(date2 - date1);
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        // console.log(diffTime + " milliseconds");
        var deliverytat = diffDays
        // console.log("deliverytat@@---",deliverytat);

        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
          //  console.log("tat1---",tat)
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
          //  console.log("tat2---",tat)
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
          //  console.log("tat3---",tat)
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
          //  console.log("tat4---",tat)
        } else {
          var aging = '';
          // console.log("tat5---",tat)
        }
      }

      var packages = data.packages ? data.packages : [];
      if (packages != '') {
        for (let pack of packages) {
          var packages_description = pack.description ? pack.description : null;
          var packages_quantity = pack.quantity ? pack.quantity : null;
          var packages_actual_weight = pack.actual_weight ? pack.actual_weight : null;
          var packages_price = pack.price ? pack.price : null;
          var total_quantity = 0, total_weight = 0, total_price = 0;
          // console.log("packages_quantity---",packages_quantity)
          total_price += (packages_price != '' ? parseFloat(packages_price) : 0);//product_value
          total_quantity += !isNumeric(packages_quantity) ? 1 : packages_quantity;
          total_weight += !isNumeric(packages_actual_weight) ? 0 : packages_actual_weight;
          // console.log("total_price---",total_price)
        }

      }

      var order_type = "FORWARD";
      //  console.log("package_rp---",package_rp)
      if (status_rp && status_rp == 1) {
        var order_type = 'REVERSE';
        // console.log("order_type---",order_type)
      }

      await pendingPickup.push({
        airwaybilno: data.airwaybilno,
        sync_source: sync_source,
        import_date: import_date,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consigneetelephone2: consigneetelephone2,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        packages_actual_weight: total_weight,
        order_type: order_type,
        packages_description: packages_description,
        packages_quantity: total_quantity,
        packages_product_value: total_price,
        payment_paytype: payment_paytype,
        order_type: order_type,
        delivery_tat: deliverytat,
        aging: aging,
        latest_undelivered_status_remark: "PENDING PICKUP",
      })
    }

    var date = moment(new Date()).format('yyyy-MM-DD');
    var hostname = req.headers.host;
    var currentPath = process.cwd();
    var dir = currentPath + '/src/public/Pending_Pickup_Ageing_Reports/';
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }
    var fileName = 'Pending_Pickup_Ageing_Report_' + startDate + '_' + endDate + '.csv';
    const csvWriter = await createCsvWriter({

      // Output csv file name is geek_data
      path: dir + fileName,
      options:
        { flags: 'a' },
      header: [

        // Title of the columns (column_names)
        { id: 'sync_source', title: 'Data Source' },
        { id: 'order_type', title: 'Order type' },
        { id: 'airwaybilno', title: 'AirwayBill No' },
        { id: 'orderno', title: 'Order No' },
        { id: 'import_date', title: 'Order Date' },
        { id: 'courier_code', title: 'Courier Code' },
        { id: 'consigneefirstname', title: 'Consignee Name' },
        { id: 'consigneeaddress1', title: 'Consignee Address1' },
        { id: 'consigneeaddress2', title: 'Consignee Address2' },
        { id: 'consigneecity', title: 'Destination City' },
        { id: 'consigneestate', title: 'State' },
        { id: 'consigneepincode', title: 'Pincode' },
        { id: 'consigneetelephone1', title: 'Contact No' },
        { id: 'payment_paytype', title: 'Pay Type' },
        { id: 'packages_quantity', title: 'Quantity' },
        { id: 'packages_description', title: 'Item Description' },
        { id: 'packages_actual_weight', title: 'Weight' },
        { id: 'packages_product_value', title: 'Product Value' },
        { id: 'consignorname', title: 'Client Name' },
        { id: 'consignorsub_vendorname', title: 'Vendor Name' },
        { id: 'consignorsub_vendorphone', title: 'Vendor Address' },
        { id: 'consignorsub_vendorcity', title: 'Vendor Pickup City' },
        { id: 'consignorsub_vendorpincode', title: 'Vendor Pincode' },
        { id: 'consignorsub_vendorphone', title: 'Vendor Contact No' },
        { id: 'aging', title: 'Aging' },
        { id: 'delivery_tat', title: 'TAT (In Days)' },
        { id: 'latest_undelivered_status_remark', title: 'Last Undelivered Reason' },
      ]
    });
    csvWriter
      .writeRecords(pendingPickup)
      .then(() => console.log('Data uploaded into csv successfully'));
    res.json({
      status: 200,
      data: 'http://' + hostname + '/Pending_Pickup_Ageing_Reports/' + fileName
    })
  } catch (error) {
    console.log(`Error: ${error.message}`)
    return res.status(400).json({
      message: 'Bad Request',
    })
  }
})

/*
    Get reverse pending pickup report using date range (import_date and booking_date)   (Date: 07-02-2022)
*/

router.get('/getReversePendingPickups/:startDate/:endDate', async (req, res) => {
  var startDate = trim(req.params.startDate)
  var endDate = trim(req.params.endDate)
  const reversePickup = [];
  try {
    const OrdersV1 = await ordersV1Model.find({
      "status.import_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      "package.rp": 1,
    });
    console.log("OrdersV1-----", OrdersV1.length)
    for await (let data of OrdersV1) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'Instashipin V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        var consignorrto = data.consignor.rto ? data.consignor.rto : null;
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null
        var status_import_date = data.status.import_date ? data.status.import_date : null;

        if (status_import_date) {
          var import_date = moment(status_import_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }

        const date1 = new Date();
        // console.log("date1----",date1)
        const date2 = new Date(status_import_date);
        const diffTime = Math.abs(date2 - date1);
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        // console.log(diffTime + " milliseconds");
        var deliverytat = diffDays
        // console.log("deliverytat@@---",deliverytat);

        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
          //  console.log("tat1---",tat)
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
          //  console.log("tat2---",tat)
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
          //  console.log("tat3---",tat)
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
          //  console.log("tat4---",tat)
        } else {
          var aging = '';
          // console.log("tat5---",tat)
        }

      }

      if (data.package) {
        var package_actual_weight = data.package.actual_weight ? data.package.actual_weight : null;
        var package_item_description = data.package.item_description ? data.package.item_description : null;
        var package_items = data.package.items ? data.package.items : null;
        var package_pieces = data.package.pieces ? data.package.pieces : null;
        var package_product = data.package.product ? data.package.product : null;
        var package_product_value = data.package.product_value ? data.package.product_value : null;
        var package_rd = data.package.rd ? data.package.rd : null;
        var package_rp = data.package.rp ? data.package.rp : null;
        // console.log("package_rp----", package_rp)
        if (package_items) {
          for (let item of package_items) {
            // console.log("description----",item.description);
            var itemdescription = item.description ? item.description : null;
            var itemquantity = item.quantity ? item.quantity : null;
            var itemprice = item.price ? item.price : null;
            var itemsku = item.sku ? item.sku : null;
            var itemcreated_datetime = item.created_datetime ? item.created_datetime : null;
          }
        }

      }


      await reversePickup.push({
        airwaybilno: airwaybilno,
        sync_source: sync_source,
        import_date: import_date,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        packages_actual_weight: package_actual_weight,
        packages_description: package_item_description,
        packages_quantity: package_pieces,
        packages_product_value: package_product_value,
        payment_paytype: package_product,
        order_type: "Reverse",
        delivery_tat: deliverytat,
        aging: aging,
        latest_undelivered_status_remark: "PENDING PICKUP",
      })
    }
    const OrdersV2 = await ordersV2Model.find({
      "status.booking_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      "status.rp": 1,
    })
    console.log("OrdersV2---", OrdersV2.length)
    for await (let data of OrdersV2) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        var consignorrto = data.consignor.rto ? data.consignor.rto : null;
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_rp = data.status.rp ? data.status.rp : null;
        // console.log("status_rp----", status_rp)
        if (status_booking_date) {
          var import_date = moment(status_booking_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }


        const date1 = new Date();
        // console.log("date1----",date1)
        const date2 = new Date(status_booking_date);
        const diffTime = Math.abs(date2 - date1);
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        // console.log(diffTime + " milliseconds");
        var deliverytat = diffDays
        // console.log("deliverytat@@---",deliverytat);

        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
          //  console.log("tat1---",tat)
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
          //  console.log("tat2---",tat)
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
          //  console.log("tat3---",tat)
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
          //  console.log("tat4---",tat)
        } else {
          var aging = '';
          // console.log("tat5---",tat)
        }


      }

      var packages = data.packages ? data.packages : [];
      if (packages != '') {
        for (let pack of packages) {
          var packages_description = pack.description ? pack.description : null;
          var packages_quantity = pack.quantity ? pack.quantity : null;
          var packages_actual_weight = pack.actual_weight ? pack.actual_weight : null;
          var packages_price = pack.price ? pack.price : null;

          var total_quantity = 0, total_weight = 0, total_price = 0;
          // console.log("packages_quantity---",packages_quantity)
          total_price += (packages_price != '' ? parseFloat(packages_price) : 0);//product_value
          total_quantity += !isNumeric(packages_quantity) ? 1 : packages_quantity;
          total_weight += !isNumeric(packages_actual_weight) ? 0 : packages_actual_weight;
          // console.log("total_price---",total_price)
        }

      }


      await reversePickup.push({
        airwaybilno: data.airwaybilno,
        sync_source: sync_source,
        import_date: import_date,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consigneetelephone2: consigneetelephone2,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        packages_actual_weight: total_weight,
        order_type: 'Reverse',
        packages_description: packages_description,
        packages_quantity: total_quantity,
        packages_product_value: total_price,
        payment_paytype: payment_paytype,
        delivery_tat: deliverytat,
        aging: aging,
        latest_undelivered_status_remark: "PENDING PICKUP",
      })
    }


    var date = moment(new Date()).format('yyyy-MM-DD');
    var hostname = req.headers.host;
    var currentPath = process.cwd();
    var dir = currentPath + '/src/public/Reverse_Pending_Pickups_Reports/';
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }
    var fileName = 'Reverse_Pending_Pickup_' + startDate + '_' + endDate + '.csv';
    const csvWriter = await createCsvWriter({

      // Output csv file name is geek_data
      path: dir + fileName,
      options:
        { flags: 'a' },
      header: [

        // Title of the columns (column_names)
        { id: 'sync_source', title: 'Data Source' },
        { id: 'order_type', title: 'Order type' },
        { id: 'airwaybilno', title: 'AirwayBill No' },
        { id: 'orderno', title: 'Order No' },
        { id: 'import_date', title: 'Order Date' },
        { id: 'courier_code', title: 'Courier Code' },
        { id: 'consigneefirstname', title: 'Consignee Name' },
        { id: 'consigneeaddress1', title: 'Consignee Address1' },
        { id: 'consigneeaddress2', title: 'Consignee Address2' },
        { id: 'consigneecity', title: 'Destination City' },
        { id: 'consigneestate', title: 'State' },
        { id: 'consigneepincode', title: 'Pincode' },
        { id: 'consigneetelephone1', title: 'Contact No' },
        { id: 'payment_paytype', title: 'Pay Type' },
        { id: 'packages_quantity', title: 'Quantity' },
        { id: 'packages_description', title: 'Item Description' },
        { id: 'packages_actual_weight', title: 'Weight' },
        { id: 'packages_product_value', title: 'Product Value' },
        { id: 'consignorname', title: 'Client Name' },
        { id: 'consignorsub_vendorname', title: 'Vendor Name' },
        { id: 'consignorsub_vendorphone', title: 'Vendor Address' },
        { id: 'consignorsub_vendorcity', title: 'Vendor Pickup City' },
        { id: 'consignorsub_vendorpincode', title: 'Vendor Pincode' },
        { id: 'consignorsub_vendorphone', title: 'Vendor Contact No' },
        { id: 'aging', title: 'Aging' },
        { id: 'delivery_tat', title: 'TAT (In Days)' },
        { id: 'latest_undelivered_status_remark', title: 'Last Undelivered Reason' },
      ]
    });
    csvWriter
      .writeRecords(reversePickup)
      .then(() => console.log('Data uploaded into csv successfully'));
    res.json({
      status: 200,
      data: 'http://' + hostname + '/Reverse_Pending_Pickups_Reports/' + fileName
    })
  } catch (error) {
    console.log(`Error: ${error.message}`)
    return res.status(400).json({
      message: 'Bad Request',
    })
  }
})

/*
    Get In-Transit report using date range (import_date and booking_date)   (Date: 08-02-2022)
*/
router.get('/getIntransitReport/:startDate/:endDate', async (req, res) => {
  var startDate = trim(req.params.startDate)
  var endDate = trim(req.params.endDate)
  const intransit = [];
  try {
    const OrdersV1 = await ordersV1Model.find({
      "status.import_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      "status.dispatch_status": '2',
    });
    console.log("OrdersV1-----", OrdersV1.length)
    for await (let data of OrdersV1) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'Instashipin V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        // console.log("consignorname---",consignorname)
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        if (status_import_date) {
          var import_date = moment(status_import_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }

        const date1 = new Date();
        // console.log("date1----",date1)
        const date2 = new Date(status_pickup_date);
        const diffTime = Math.abs(date2 - date1);
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        // console.log(diffTime + " milliseconds");
        var deliverytat = diffDays
        // console.log("deliverytat@@---",deliverytat);

        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
          //  console.log("tat1---",tat)
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
          //  console.log("tat2---",tat)
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
          //  console.log("tat3---",tat)
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
          //  console.log("tat4---",tat)
        } else {
          var aging = '';
          // console.log("tat5---",tat)
        }

      }
      if (data.package) {
        var package_actual_weight = data.package.actual_weight ? data.package.actual_weight : null;
        var package_item_description = data.package.item_description ? data.package.item_description : null;
        var package_items = data.package.items ? data.package.items : null;
        var package_pieces = data.package.pieces ? data.package.pieces : null;
        var package_product = data.package.product ? data.package.product : null;
        var product_value = data.package.product_value ? data.package.product_value : null;
        var package_rp = data.package.rp ? data.package.rp : null;
        var collectable_value = data.package.collectable_value ? data.package.collectable_value : null;
        if (package_items) {
          for (let item of package_items) {
            // console.log("description----",item.description);
            var itemdescription = item.description ? item.description : null;
            var itemquantity = item.quantity ? item.quantity : null;
          }
        }

      }


      var order_type = "FORWARD";
      //  console.log("package_rp---",package_rp)
      if (package_rp && package_rp == 1) {
        var order_type = 'REVERSE';
        // console.log("order_type---",order_type)
      }

      await intransit.push({
        airwaybilno: airwaybilno,
        sync_source: sync_source,
        import_date: import_date,
        pickup_date: pickup_date,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        packages_description: package_item_description,
        packages_quantity: package_pieces,
        product_value: product_value,
        payment_paytype: package_product,
        collectable_value: collectable_value,
        order_type: order_type,
        delivery_tat: deliverytat,
        aging: aging,
        latest_status: "IN-TRANSIT",
      })
    }
    const OrdersV2 = await ordersV2Model.find({
      "status.booking_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      "status.dispatch_status": '2',
    })

    console.log("OrdersV2---", OrdersV2.length)
    for await (let data of OrdersV2) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;

        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
        var collectable_value = data.payment.collectable ? data.payment.collectable : null;
        var product_value = data.payment.invoice_amount ? data.payment.invoice_amount : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_rp = data.status.rp ? data.status.rp : null;
        if (status_booking_date) {
          var import_date = moment(status_booking_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        const date1 = new Date();
        // console.log("date1----",date1)
        const date2 = new Date(status_booking_date);
        const diffTime = Math.abs(date2 - date1);
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        // console.log(diffTime + " milliseconds");
        var deliverytat = diffDays
        // console.log("deliverytat@@---",deliverytat);

        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
          //  console.log("tat1---",tat)
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
          //  console.log("tat2---",tat)
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
          //  console.log("tat3---",tat)
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
          //  console.log("tat4---",tat)
        } else {
          var aging = '';
          // console.log("tat5---",tat)
        }
      }

      var packages = data.packages ? data.packages : [];
      if (packages != '') {
        for (let pack of packages) {
          var packages_description = pack.description ? pack.description : null;
          var packages_quantity = pack.quantity ? pack.quantity : null;
          var packages_actual_weight = pack.actual_weight ? pack.actual_weight : null;
          var packages_price = pack.price ? pack.price : null;
          // var total_quantity = 0, total_weight = 0, total_price = 0;
          // // console.log("packages_quantity---",packages_quantity)
          // total_price += (packages_price != '' ? parseFloat(packages_price) : 0);//product_value
          // total_quantity += !isNumeric(packages_quantity) ? 1 : packages_quantity;
          // total_weight += !isNumeric(packages_actual_weight) ? 0 : packages_actual_weight;
          // console.log("total_price---",total_price)
        }

      }

      var order_type = "FORWARD";

      await intransit.push({
        airwaybilno: data.airwaybilno,
        sync_source: sync_source,
        import_date: import_date,
        pickup_date: pickup_date,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consigneetelephone2: consigneetelephone2,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        order_type: order_type,
        packages_description: packages_description,
        collectable_value: collectable_value,
        product_value: product_value,
        payment_paytype: payment_paytype,
        order_type: order_type,
        delivery_tat: deliverytat,
        aging: aging,
        latest_status: "IN-TRANSIT",
      })
    }

    var date = moment(new Date()).format('yyyy-MM-DD');
    var hostname = req.headers.host;
    var currentPath = process.cwd();
    var dir = currentPath + '/src/public/Intransit_Reports/';
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }
    var fileName = 'Intransit_Report_' + startDate + '_' + endDate + '.csv';
    const csvWriter = await createCsvWriter({

      // Output csv file name is geek_data
      path: dir + fileName,
      options:
        { flags: 'a' },
      header: [

        // Title of the columns (column_names)
        { id: 'sync_source', title: 'Data Source' },
        { id: 'order_type', title: 'Order type' },
        { id: 'airwaybilno', title: 'AirwayBill No' },
        { id: 'orderno', title: 'Order No' },
        { id: 'courier_code', title: 'Courier Code' },
        { id: 'import_date', title: 'Import Date' },
        { id: 'pickup_date', title: 'Pickup Date' },
        { id: 'consignorname', title: 'Client Name' },
        { id: 'consignorsub_vendorname', title: 'Vendor Name' },
        { id: 'payment_paytype', title: 'Pay Type' },
        { id: 'collectable_value', title: 'Collectable Value' },
        { id: 'product_value', title: 'Product Value' },
        { id: 'packages_description', title: 'Product Title' },
        { id: 'consigneefirstname', title: 'Consignee Name' },
        { id: 'consigneeaddress1', title: 'Consignee Address1' },
        { id: 'consigneecity', title: 'Destination City' },
        { id: 'consigneestate', title: 'State' },
        { id: 'consigneepincode', title: 'Pincode' },
        { id: 'consigneetelephone1', title: 'Consignee Phone 1' },
        { id: 'latest_status', title: 'Latest Status' },
        { id: 'aging', title: 'Intransit Ageing' },
        { id: 'delivery_tat', title: 'TAT (In Days)' },
      ]
    });
    csvWriter
      .writeRecords(intransit)
      .then(() => console.log('Data uploaded into csv successfully'));
    res.json({
      status: 200,
      data: 'http://' + hostname + '/Intransit_Reports/' + fileName
    })
  } catch (error) {
    console.log(`Error: ${error.message}`)
    return res.status(400).json({
      message: 'Bad Request',
    })
  }
})

/*
    Get OFD(out for delivery) report using date range (import_date and booking_date)   (Date: 09-02-2022)
*/
router.get('/getOutForDeliveryReport/:startDate/:endDate', async (req, res) => {
  var startDate = trim(req.params.startDate)
  var endDate = trim(req.params.endDate)
  const outForDelivery = [];
  try {
    const OrdersV1 = await ordersV1Model.find({
      "status.import_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      "status.dispatch_status": '4',
    });
    console.log("OrdersV1-----", OrdersV1.length)
    for await (let data of OrdersV1) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'Instashipin V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        // console.log("consignorname---",consignorname)
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        if (status_import_date) {
          var import_date = moment(status_import_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }

        const date1 = new Date();
        // console.log("date1----",date1)
        const date2 = new Date(status_pickup_date);
        const diffTime = Math.abs(date2 - date1);
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        // console.log(diffTime + " milliseconds");
        var deliverytat = diffDays
        // console.log("deliverytat@@---",deliverytat);

        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
          //  console.log("tat1---",tat)
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
          //  console.log("tat2---",tat)
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
          //  console.log("tat3---",tat)
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
          //  console.log("tat4---",tat)
        } else {
          var aging = '';
          // console.log("tat5---",tat)
        }

      }
      if (data.package) {
        var package_actual_weight = data.package.actual_weight ? data.package.actual_weight : null;
        var package_item_description = data.package.item_description ? data.package.item_description : null;
        var package_items = data.package.items ? data.package.items : null;
        var package_pieces = data.package.pieces ? data.package.pieces : null;
        var package_product = data.package.product ? data.package.product : null;
        var product_value = data.package.product_value ? data.package.product_value : null;
        var package_rp = data.package.rp ? data.package.rp : null;
        var collectable_value = data.package.collectable_value ? data.package.collectable_value : null;

      }

      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var count = 0;
      count = tracking.length;
      if (count > 0) {
        count = count - 1;
        var status_remark = tracking[count]['remarks'];
      } else {
        var status_remark = 'PENDING PICKUP';
      }
      // console.log("status_remark---",status_remark)

      var order_type = "FORWARD";
      //  console.log("package_rp---",package_rp)
      if (package_rp && package_rp == 1) {
        var order_type = 'REVERSE';
        // console.log("order_type---",order_type)
      }

      await outForDelivery.push({
        airwaybilno: airwaybilno,
        sync_source: sync_source,
        import_date: import_date,
        pickup_date: pickup_date,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneelastname: consigneelastname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        packages_description: package_item_description,
        packages_quantity: package_pieces,
        product_value: product_value,
        payment_paytype: package_product,
        collectable_value: collectable_value,
        order_type: order_type,
        delivery_tat: deliverytat,
        aging: aging,
        latest_status: "OUT FOR DELIVERY",
        status_remark: status_remark
      })
    }
    const OrdersV2 = await ordersV2Model.find({
      "status.booking_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      "status.dispatch_status": '4',
    })

    console.log("OrdersV2---", OrdersV2.length)
    for await (let data of OrdersV2) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;

        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
        var collectable_value = data.payment.collectable ? data.payment.collectable : null;
        var product_value = data.payment.invoice_amount ? data.payment.invoice_amount : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_rp = data.status.rp ? data.status.rp : null;
        if (status_booking_date) {
          var import_date = moment(status_booking_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        const date1 = new Date();
        // console.log("date1----",date1)
        const date2 = new Date(status_booking_date);
        const diffTime = Math.abs(date2 - date1);
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        // console.log(diffTime + " milliseconds");
        var deliverytat = diffDays
        // console.log("deliverytat@@---",deliverytat);

        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
          //  console.log("tat1---",tat)
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
          //  console.log("tat2---",tat)
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
          //  console.log("tat3---",tat)
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
          //  console.log("tat4---",tat)
        } else {
          var aging = '';
          // console.log("tat5---",tat)
        }
      }

      var packages = data.packages ? data.packages : [];
      if (packages != '') {
        for (let pack of packages) {
          var packages_description = pack.description ? pack.description : null;
          var packages_quantity = pack.quantity ? pack.quantity : null;
          var packages_actual_weight = pack.actual_weight ? pack.actual_weight : null;
          var packages_price = pack.price ? pack.price : null;
          var total_quantity = 0, total_weight = 0, total_price = 0;
          // console.log("packages_quantity---",packages_quantity)
          total_price += (packages_price != '' ? parseFloat(packages_price) : 0);//product_value
          total_quantity += !isNumeric(packages_quantity) ? 1 : packages_quantity;
          total_weight += !isNumeric(packages_actual_weight) ? 0 : packages_actual_weight;
          // console.log("total_price---",total_price)
        }

      }

      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var count = 0;
      count = tracking.length;
      if (count > 0) {
        count = count - 1;
        var status_remark = tracking[count]['remarks'];
      } else {
        var status_remark = 'PENDING PICKUP';
      }
      // console.log("status_remark---",status_remark)
      var order_type = "FORWARD";

      await outForDelivery.push({
        airwaybilno: data.airwaybilno,
        sync_source: sync_source,
        import_date: import_date,
        pickup_date: pickup_date,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneelastname: consigneelastname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consigneetelephone2: consigneetelephone2,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        order_type: order_type,
        packages_description: packages_description,
        // total_price: total_price,
        product_value: total_price,
        payment_paytype: payment_paytype,
        order_type: order_type,
        delivery_tat: deliverytat,
        aging: aging,
        latest_status: "OUT FOR DELIVERY",
        status_remark: status_remark
      })
    }

    var date = moment(new Date()).format('yyyy-MM-DD');
    var hostname = req.headers.host;
    var currentPath = process.cwd();
    var dir = currentPath + '/src/public/OFD_delivery_Ageing_Reports/';
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }
    var fileName = 'OFD_delivery_Ageing_Report_' + startDate + '_' + endDate + '.csv';
    const csvWriter = await createCsvWriter({

      // Output csv file name is geek_data
      path: dir + fileName,
      options:
        { flags: 'a' },
      header: [

        // Title of the columns (column_names)
        { id: 'consignorname', title: 'Client Name' },
        { id: 'airwaybilno', title: 'AirwayBill No' },
        { id: 'airwaybilno', title: 'Invice No' },
        { id: 'orderno', title: 'Order No' },
        { id: 'courier_code', title: 'Courier Name' },
        { id: 'consigneefirstname', title: 'Consignee First Name' },
        { id: 'consigneelastname', title: 'Consignee Last Name' },
        { id: 'consigneeaddress1', title: 'Consignee Address' },
        { id: 'consigneecity', title: 'Destination' },
        { id: 'consigneestate', title: 'State' },
        { id: 'consigneepincode', title: 'Destination Pincode' },
        { id: 'consigneetelephone1', title: 'Consignee Phone 1' },
        { id: 'packages_description', title: 'Item Description' },
        { id: 'product_value', title: 'Product Amount' },
        { id: 'consignorsub_vendoraddress1', title: 'Pickup Location' },
        { id: 'import_date', title: 'Import Date' },
        { id: 'pickup_date', title: 'Pickup Date' },
        { id: 'aging', title: 'OFD Ageing' },
        { id: 'delivery_tat', title: 'TAT (In Days)' },
        { id: 'latest_status', title: 'Latest Status' },
        { id: 'status_remark', title: 'Status Remark' },
      ]
    });
    csvWriter
      .writeRecords(outForDelivery)
      .then(() => console.log('Data uploaded into csv successfully'));
    res.json({
      status: 200,
      data: 'http://' + hostname + '/OFD_delivery_Ageing_Reports/' + fileName
    })
  } catch (error) {
    console.log(`Error: ${error.message}`)
    return res.status(400).json({
      message: 'Bad Request',
    })
  }
})
  
/*
    Get NDR Ageing report using date range (last_attempt_date)   (Date: 09-02-2022)
*/

router.get('/getNDRReport/:startDate/:endDate', async (req, res) => {
  var startDate = trim(req.params.startDate)
  var endDate = trim(req.params.endDate)
  const ndrReport = [];
  try {
    const OrdersV1 = await ordersV1Model.find({
      "status.last_attempt_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      $or: [
        {
          "status.dispatch_status": '6'
        },
        {
          "status.dispatch_status": '7'
        },
        {
          "status.dispatch_status": '18'
        },
      ],
    });
    console.log("OrdersV1---", OrdersV1.length)
    for await (let data of OrdersV1) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var api_serviceable_list = api_serviceable_list ? api_serviceable_list : null
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'Instashipin V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        var consignorrto = data.consignor.rto ? data.consignor.rto : null;
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }
      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;
        var status_last_attempt_date = data.status.last_attempt_date ? data.status.last_attempt_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_rto_delivery_date = data.status.rto_delivery_date ? data.status.rto_delivery_date : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_rto_intransit_date = data.status.rto_intransit_date ? data.status.rto_intransit_date : null;
        var status_rto_tracking_no = data.status.rto_tracking_no ? data.status.rto_tracking_no : null;
        var status_zone_code = data.status.zone_code ? data.status.zone_code : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_reattempt_date = data.status.reattempt_date ? data.status.reattempt_date : null;


      }
      if (data.weight_update) {
        var weightupdate_actual_weight = data.weight_update.actual_weight ? data.weight_update.actual_weight : null;
      }
      if (data.package) {
        var package_actual_weight = data.package.actual_weight ? data.package.actual_weight : null;
        var package_item_description = data.package.item_description ? data.package.item_description : null;
        var package_items = data.package.items ? data.package.items : null;
        var package_pieces = data.package.pieces ? data.package.pieces : null;
        var package_product = data.package.product ? data.package.product : null;
        var package_rp = data.package.rp ? data.package.rp : null;
        var package_volumetric_weight = data.package.volumetric_weight ? data.package.volumetric_weight : null;
        var package_collectable_value = data.package.collectable_value ? data.package.collectable_value : null;

      }
      if (data.payments) {
        var payments_cod_collect = data.payments.cod_collect ? data.payments.cod_collect : null;
        if (payments_cod_collect) {
          var payments_codcollect_cod_recived = data.payments.cod_collect.cod_recived ? data.payments.cod_collect.cod_recived : null;
        }
        var payments_cod_payment = data.payments.cod_payment ? data.payments.cod_payment : null;
        if (payments_cod_payment) {
          var payments_codpayment_cod_paid = data.payments.cod_payment.cod_paid ? data.payments.cod_payment.cod_paid : null;
          var payments_codpayment_paidbankrefno = data.payments.cod_payment.paid_bank_ref_no ? data.payments.cod_payment.paid_bank_ref_no : null;
          var payments_codpayment_paiddatetime = data.payments.cod_payment.paid_datetime ? data.payments.cod_payment.paid_datetime : null;
          if (payments_codpayment_paiddatetime) {
            var payments_codpayment_paiddate = moment(payments_codpayment_paiddatetime).format('YYYY-MM-DD');
          } else {
            var payments_codpayment_paiddate = '';
          }
        }

      }

      var ndr_ticket = data.ndr_ticket ? data.ndr_ticket : [];
      if (ndr_ticket != '') {
        var ndr_ticket_created_datetime = data['ndr_ticket']['ndr_ticket_created_datetime'] ? data['ndr_ticket']['ndr_ticket_created_datetime'] : null;
        var ndr_master_status = data['ndr_ticket']['ndr_master']['ndr_master_status'] ? data['ndr_ticket']['ndr_master']['ndr_master_status'] : null;
        var ndr_master_updated_datetime = data['ndr_ticket']['ndr_master']['ndr_master_updated_datetime'] ? data['ndr_ticket']['ndr_master']['ndr_master_updated_datetime'] : null;
        // console.log("ndr_ticket---",ndr_ticket)
      }


      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var first_ofd_date = sec_ofd_date = third_ofd_date = last_ofd_date = '';
      var i = 0;
      if (tracking != '') {
        // console.log("tracking---",tracking)
        var statuscode = '305';
        var no_of_attempt = 0;
        var tracking_counts = [];
        var obj = {}
        tracking.forEach(function (item) {
          obj[item.status_code] ? obj[item.status_code]++ : obj[item.status_code] = 1;
        });
        // console.log("obj----",obj)  
        // console.log("count---",obj[statuscode])
        no_of_attempt = obj[statuscode];
        // console.log("tracking[0]----",tracking[0])
        var latest_status = '';
        if (status_dispatch_status == '0') {
          latest_status = "PENDING PICKUP";
        } else if (status_dispatch_status == '1') {
          latest_status = "PICKUP DONE";
        } else if (status_dispatch_status == '2') {
          latest_status = "IN-TRANSIT";
        } else if (status_dispatch_status == '4') {
          latest_status = "OUT FOR DELIVERY"
        } else if (status_dispatch_status == '5') {
          latest_status = "DELIVERED"
          // console.log("latest_status---",latest_status)
        } else if (status_dispatch_status == '6') {
          latest_status = "UNDELIVERED"
        } else if (status_dispatch_status == '7') {
          latest_status = "UNDELIVERED"
        } else if (status_dispatch_status == '10') {
          latest_status = "RTO INITIATED"
        } else if (status_dispatch_status == '11') {
          latest_status = "RTO IN-TRANSIT"
        } else if (status_dispatch_status == '12') {
          latest_status = "RTO OUT FOR DELIVERY"
        } else if (status_dispatch_status == '13') {
          latest_status = "RTO DELIVERED"
        } else if (status_dispatch_status == '14') {
          latest_status = "PICKUP CANCEL BY CLIENT"
        } else if (status_dispatch_status == '15') {
          latest_status = "LOST"
        } else if (status_dispatch_status == '16') {
          latest_status = "SHIPMENT DAMAGE"
        } else if (status_dispatch_status == '17') {
          latest_status = "DANGER GOODS"
        } else if (status_dispatch_status == '18') {
          latest_status = "REATTEMPT"
        } else if (status_dispatch_status == '19') {
          latest_status = "RTO"
        } else if (status_dispatch_status == '20') {
          latest_status = "RTO"
        } else if (status_dispatch_status == '21') {
          latest_status = "SELF COLLECT (CS)"
        } else if (status_dispatch_status == '55') {
          latest_status = "RTO UNDELIVERED"
        } else if (status_dispatch_status == '56') {
          latest_status = "REVERSE UNDELIVERED"
        } else if (status_dispatch_status == '1001') {
          latest_status = "UNATTEMPTED"
        } else if (status_dispatch_status == '8') {
          latest_status = "UNDELIVERED"
        } else {
          latest_status = "PENDING PICKUP";
          // status_remark="PENDING PICKUP";
        }
        var ofd_cnt = "0";
        var ndr_sd_remarks = "";
        var ndr_sd = "";
        var status_remark = "";
        var count = 0;
        count = tracking.length;
        var client_reattempt_date, client_comment = ''
        if (count > 0) {
          count = count - 1;
          var status_remark = tracking[count]['remarks'];
          if (status_reattempt_date && status_reattempt_date != null) {
            client_reattempt_date = status_reattempt_date;
          }
          else {
            client_reattempt_date = tracking[count]['client_reattempt_date'];;
          }

          if (status_dispatch_status == 'I002') {
            //$client_comment=$udx['attributes']['tracking'][$tot_cn]['remarks'];
            client_comment = tracking[count]['client_comment'];
          }
          else {
            client_comment = tracking[count]['client_comment'];
          }
        } else {
          var status_remark = 'PENDING PICKUP';
          client_comment = '';
          client_reattempt_date = '';
        }
        for await (let track of tracking) {
          // var track_lsp_status_code = track.lsp_status_code ? track.lsp_status_code : null;
          var track_status = track.status ? track.status : null;
          var track_status_code = track.status_code ? track.status_code : null;
          // var track_lsp_status = track.lsp_status ? track.lsp_status : null;
          var track_parent_status_code = track.parent_status_code ? track.parent_status_code : null;
          // var track_source_status_code = track.source_status_code ? track.source_status_code : null;
          var track_remarks = track.remarks ? track.remarks : null;
          var track_updated_date = track.updated_date ? track.updated_date : null;
          var track_location = track.location ? track.location : null;
          // var track_manualentry = track.manualentry ? track.manualentry : null;
          // console.log("track_status_code.length----",track_status_code.length)

          // console.log("track_status_code----",track_status_code)
          // if (track_parent_status_code == "4") {
          //   var last_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
          // } else {
          //   var last_ofd_date = '';
          // }


          if (track_status_code == '305') {
            if (i == 0) {
              var first_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("first_ofd_date----",first_ofd_date)
            } else if (i == 1) {
              var sec_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("sec_ofd_date----",sec_ofd_date)
            } else if (i == 2) {
              var third_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("third_ofd_date----",third_ofd_date)
            }
            var last_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
            // var openNDR_curr = new Date();
            // var openNDR_ofd = new Date(last_ofd_date);
            // const diffTime = Math.abs(openNDR_ofd - openNDR_curr);
            // const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
            // // console.log(diffTime + " milliseconds");
            // var open_ndr_tat = diffDays
            i++;
            // console.log("i---",i)
          } else {
            var first_ofd_date = first_ofd_date;
            var sec_ofd_date = sec_ofd_date;
            var third_ofd_date = third_ofd_date;
            var last_ofd_date = last_ofd_date;
          }


          if (track_parent_status_code == "10" || track_parent_status_code == "18") {
            // var last = [];
            // $removed = array_pop($udx['attributes']['tracking']);
            // var result1 = tracking;
            // last['remarks'] = end(result1);
            var ndr_reason_for_reatttempt = track_remarks;
          }

          if (track_status_code != '' && (track_status_code == "I002" || track_status_code == "I004" || track_status_code == "I005" || track_status_code == "I006")) {
            // $latest_status = 'UNDELIVERED';
            var ndr_action = track_status;
            var ndr_action_date = moment(track_updated_date).format('YYYY-MM-DD');
          }
          var ndr_action_date = moment(track_updated_date).format('YYYY-MM-DD');
          // const date1 = new Date();
          // const date2 = new Date(ndr_action_date);
          // const diffTime = Math.abs(date2 - date1);
          // const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          // // console.log(diffTime + " milliseconds");
          // var actionTAT = diffDays
          // console.log("deliverytat@@---",deliverytat + " days");



          const masters = [
            { dispatch_status_code: 99, status: "PENDING PICKUP", remarks: "pending pickup", dispatch_status: 0 },
            { dispatch_status_code: 100, status: "PICKUP DONE", remarks: "pickup done", dispatch_status: 1 },
            { dispatch_status_code: 102, status: "IN-TRANSIT", remarks: "processing at origin hub", dispatch_status: 2 },
            { dispatch_status_code: 305, status: "OUT FOR DELIVERY", remarks: "out for delivery", dispatch_status: 4 },
            { dispatch_status_code: 400, status: "Delivered", remarks: "delivered", dispatch_status: 5 },
            { dispatch_status_code: 401, status: "RTO Delivered", remarks: "rto delivered", dispatch_status: 13 },
            { dispatch_status_code: 500, status: "UNDELIVERED", remarks: "consignee refused", dispatch_status: 6 },
            { dispatch_status_code: 501, status: "UNDELIVERED", remarks: "incomplete address", dispatch_status: 6 },
            { dispatch_status_code: 503, status: "UNDELIVERED", remarks: "oda", dispatch_status: 6 },
            { dispatch_status_code: 504, status: "UNDELIVERED", remarks: "consignee shifted", dispatch_status: 6 },
            { dispatch_status_code: 505, status: "UNDELIVERED", remarks: "DAMAGED", dispatch_status: 6 },
            { dispatch_status_code: 506, status: "UNDELIVERED", remarks: "no such consignee", dispatch_status: 6 },
            { dispatch_status_code: 507, status: "UNDELIVERED", remarks: "future delivery", dispatch_status: 6 },
            { dispatch_status_code: 508, status: "UNDELIVERED", remarks: "cod not ready", dispatch_status: 6 },
            { dispatch_status_code: 509, status: "UNDELIVERED", remarks: "residence/office closed", dispatch_status: 6 },
            { dispatch_status_code: 510, status: "UNDELIVERED", remarks: "out of station", dispatch_status: 6 },
            { dispatch_status_code: 511, status: "UNDELIVERED", remarks: "shipment lost", dispatch_status: 6 },
            { dispatch_status_code: 512, status: "UNDELIVERED", remarks: "Dangerous Goods", dispatch_status: 6 },
            { dispatch_status_code: 513, status: "UNDELIVERED", remarks: "Self Collect", dispatch_status: 6 },
            { dispatch_status_code: 514, status: "UNDELIVERED", remarks: "Held With Govt Authority", dispatch_status: 6 },
            { dispatch_status_code: 515, status: "UNDELIVERED", remarks: "consignee not available", dispatch_status: 6 },
            { dispatch_status_code: 516, status: "UNDELIVERED", remarks: "consignee not responding", dispatch_status: 6 },
            { dispatch_status_code: 517, status: "UNDELIVERED", remarks: "misroute", dispatch_status: 6 },
            { dispatch_status_code: 518, status: "UNDELIVERED", remarks: "on hold", dispatch_status: 6 },
            { dispatch_status_code: 519, status: "UNDELIVERED", remarks: "restricted area", dispatch_status: 6 },
            { dispatch_status_code: 520, status: "UNDELIVERED", remarks: "snatched by consignee", dispatch_status: 6 },
            { dispatch_status_code: 521, status: "UNDELIVERED", remarks: "disturbance/natural disaster/strike/COVID", dispatch_status: 6 },
            { dispatch_status_code: 522, status: "UNDELIVERED", remarks: "Open Delivery", dispatch_status: 6 },
            { dispatch_status_code: 523, status: "UNDELIVERED", remarks: "Customer denied - OTP Delivery", dispatch_status: 6 },
            { dispatch_status_code: 524, status: "UNDELIVERED", remarks: "Time Constraint / Dispute", dispatch_status: 6 },
            { dispatch_status_code: 600, status: "RTO INITIATED", remarks: "rto initiated ", dispatch_status: 10 },
            { dispatch_status_code: 601, status: "RTO IN-TRANSIT", remarks: "rto intransit ", dispatch_status: 11 },
            { dispatch_status_code: 615, status: "RTO UNDELIVERED ", remarks: "Vendor refused ", dispatch_status: 55 },
            { dispatch_status_code: 900, status: "Pickup Cancelled ", remarks: "Pickup Cancelled by Client ", dispatch_status: 14 },
            { dispatch_status_code: 901, status: "LOST ", remarks: "SHIPMENT LOST ", dispatch_status: 15 },
            { dispatch_status_code: 902, status: "SHIPMENT DAMAGE ", remarks: "SHIPMENT DAMAGE ", dispatch_status: 16 },
            { dispatch_status_code: 951, status: "REATTEMPT ", remarks: "Reattempt ", dispatch_status: 18 },
            { dispatch_status_code: 1001, status: "Unattempted ", remarks: "Unattempted ", dispatch_status: 1001 },
            { dispatch_status_code: "I001", status: "SWC ", remarks: "Shared with Client ", dispatch_status: 7 },
            { dispatch_status_code: "I002", status: "CL-REATTEMPT ", remarks: "Client Reattempt ", dispatch_status: 7 },
            { dispatch_status_code: "I003", status: "CL-RTO-INITIATED ", remarks: "cl rto initiated ", dispatch_status: 7 },
            { dispatch_status_code: "I004", status: "CL-HOLD ", remarks: "Client Hold ", dispatch_status: 7 },
            { dispatch_status_code: "I005", status: "CL-SELFCOLLECT ", remarks: "Client Self Collect ", dispatch_status: 7 },
            { dispatch_status_code: "I006", status: "SD-REATTEMPT ", remarks: "Shipdelight Reattempt ", dispatch_status: 7 },
            { dispatch_status_code: "I007", status: "SD-RTO-INITIATED ", remarks: "sd rto initiated ", dispatch_status: 7 },
            { dispatch_status_code: "I008", status: "SD-SELFCOLLECT ", remarks: "Shipdelight Self Collect ", dispatch_status: 7 },
            { dispatch_status_code: "N001", status: "Whatsapp Calling ", remarks: "Whatsapp Calling ", dispatch_status: 8 },
            { dispatch_status_code: 402, status: "Partial Delivered ", remarks: "Partial Delivered ", dispatch_status: 21 },
            { dispatch_status_code: "R1207", status: "Reverse UNDELIVERED ", remarks: "on hold ", dispatch_status: 6 },
            { dispatch_status_code: "R1208", status: "Reverse UNDELIVERED ", remarks: "vendor not available ", dispatch_status: 6 },
          ]


          if (track_status_code == "305")
            // {
            ofd_cnt++;
          // console.log("track_parent_status_code---",track_parent_status_code)
          if (track_parent_status_code == '6') {
            var scode = tracking[ofd_cnt]['status_code'];
            // console.log("track_parent_status_code----",track_parent_status_code)
            // console.log("scode----",scode)
            // console.log("status_dispatch_status_code---",status_dispatch_status_code)
            var result = masters.find(c => c.dispatch_status == status_dispatch_status && c.dispatch_status_code == status_dispatch_status_code)
            if (result != undefined) {
              // console.log("result---",result.remarks)
              ndr_sd_remarks = result.remarks;
              // console.log("ndr_sd_remarks---",ndr_sd_remarks)
            } else {
              ndr_sd_remarks = '';
            }
            ndr_sd = latest_status;
            var status_remark = track_remarks;
            // console.log("ndr_sd---",ndr_sd)
            // console.log("status_remark---",status_remark)

          } else {
            ndr_sd_remarks = '';
            ndr_sd = '';
            status_remark = 'PENDING PICKUP';
            // console.log("ndr_sd@@---",ndr_sd)
            // console.log("ndr_sd_remarks@@---",ndr_sd_remarks)
          }
          // }

        }


      }



      if (status_pickup_date) {
        var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
      } else {
        var pickup_date = '';
      }

      if (status_last_attempt_date) {
        var last_attempt_date = moment(status_last_attempt_date).format('YYYY-MM-DD');
      } else {
        var last_attempt_date = '';
      }

      var actionDate = ''
      var ndr_update = '';
      if (status_reattempt_date) {
        actionDate = moment(status_reattempt_date).format('YYYY-MM-DD');
      }
      ndr_update = 'NDR instruction not received';
      var action_status = 'Instruction Pending';

      if (last_ofd_date != '' && ndr_action_date != '') {
        var ofddate = last_ofd_date;
        var actiondate = ndr_action_date;

        if (ofddate > actiondate) {
          action_status = 'Reattempt Done';
          ndr_update = 'NDR instruction followed';
        }
        if (last_ofd_date == ndr_action_date) {
          action_status = 'Reattempt Open';
        }

        //NDR Update
        if (actiondate > ofddate) {
          ndr_update = 'Reattempt pending';
        }
      } else {
        var ofddate = '';
        var actiondate = '';
      }

      if (last_ofd_date != '' && ndr_action_date == '') {
        action_status = 'Instruction Pending';
        ndr_update = 'NDR instruction not received';
      }
      if (ndr_action_date != '' && last_ofd_date == '') {
        // echo "in esleif";
        action_status = 'Reattempt Open';
      }

      var order_type = "FORWARD";
      //  console.log("package_rp---",package_rp)
      if (package_rp && package_rp == 1) {
        var order_type = 'REVERSE';
        // console.log("order_type---",order_type)
      }

      if (status_dispatch_status == '6' || (status_dispatch_status_code != 'I003' || status_dispatch_status_code != 'I007')) {
        await ndrReport.push({
          sync_source: sync_source,
          order_type: order_type,
          courier_code: courier_code,
          airwaybilno: airwaybilno,
          orderno: orderno,
          consignorname: consignorname,
          consignorsub_vendorname: consignorsub_vendorname,
          consigneefirstname: consigneefirstname,
          consigneeaddress1: consigneeaddress1,
          consigneeaddress2: consigneeaddress2,
          consigneecity: consigneecity,
          consigneestate: consigneestate,
          consigneepincode: consigneepincode,
          consigneetelephone1: consigneetelephone1,
          consigneetelephone2: consigneetelephone2,
          packages_description: package_item_description,
          payment_paytype: package_product,
          package_collectable_value: package_collectable_value,
          pickup_date: pickup_date,
          client_reattempt_date: client_reattempt_date,
          client_comment: client_comment,
          ndr_sd: ndr_sd,
          status_remark: status_remark,
          ndr_sd_remarks: ndr_sd_remarks,
          last_attempt_date: last_attempt_date,
          last_ofd_date: last_ofd_date,
          no_of_attempt: no_of_attempt,
          first_ofd_date: first_ofd_date,
          sec_ofd_date: sec_ofd_date,
          third_ofd_date: third_ofd_date,
          ndr_action: ndr_action,
          ndr_action_date: ndr_action_date,
          allocate_date: ndr_ticket_created_datetime,
          allocated_to: ndr_master_status,
          action_by: ndr_master_updated_datetime,
          actionTAT: '',
          open_ndr_tat: '',
          action_status: action_status,
          ndr_update: ndr_update,
        })
      }
    }

    const OrdersV2 = await ordersV2Model.find({
      "status.last_attempt_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      $or: [
        {
          "status.dispatch_status": '6'
        },
        {
          "status.dispatch_status": '7'
        },
        {
          "status.dispatch_status": '18'
        },
      ],
    });
    console.log("OrdersV2---", OrdersV2.length)

    for await (let data of OrdersV2) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var api_serviceable_list = api_serviceable_list ? api_serviceable_list : null
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        var consignorrto = data.consignor.rto ? data.consignor.rto : null;
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }
      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
        var payment_collectable = data.payment.collectable ? data.payment.collectable : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;
        var status_last_attempt_date = data.status.last_attempt_date ? data.status.last_attempt_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_rto_delivery_date = data.status.rto_delivery_date ? data.status.rto_delivery_date : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_rto_intransit_date = data.status.rto_intransit_date ? data.status.rto_intransit_date : null;
        var status_rto_tracking_no = data.status.rto_tracking_no ? data.status.rto_tracking_no : null;
        var status_zone_code = data.status.zone_code ? data.status.zone_code : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_reattempt_date = data.status.reattempt_date ? data.status.reattempt_date : null;


        if (status_delivery_date && status_delivery_date != "0000-00-00 00:00:00" && status_delivery_date != "" && status_pickup_date && status_pickup_date != "0000-00-00 00:00:00" && status_pickup_date != "") {
          const date1 = new Date(status_pickup_date);
          const date2 = new Date(status_delivery_date);
          const diffTime = Math.abs(date2 - date1);
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          // console.log(diffTime + " milliseconds");
          var deliverytat = diffDays
          // console.log("deliverytat@@---",deliverytat + " days");
        } else {
          var deliverytat = "";
          // console.log("deliverytat----",deliverytat)
        }
        if (deliverytat <= "6") {
          var tat = 'Day ' + deliverytat
          //  console.log("tat1---",tat)
        } else if (deliverytat > 6 && deliverytat < 16) {
          var tat = '7 To 15 Days';
          //  console.log("tat2---",tat)
        } else if (deliverytat > 15 && deliverytat < 31) {
          var tat = '16 To 30 Days';
          //  console.log("tat3---",tat)
        } else if (deliverytat > 30) {
          var tat = '>30 Days';
          //  console.log("tat4---",tat)
        } else {
          var tat = '';
          // console.log("tat5---",tat)
        }

      }
      if (data.weight_update) {
        var weightupdate_actual_weight = data.weight_update.actual_weight ? data.weight_update.actual_weight : null;
      }

      var packages = data.packages ? data.packages : [];
      if (packages != '') {
        for (let pack of packages) {
          var packages_description = pack.description ? pack.description : null;
          // var packages_sku = pack.sku ? pack.sku : null;
          var packages_quantity = pack.quantity ? pack.quantity : null;
          var packages_actual_weight = pack.actual_weight ? pack.actual_weight : null;
          var package_volumetric_weight = pack.volumetric_weight ? pack.volumetric_weight : null;
          // var packages_length = pack.length ? pack.length : null;
          // var packages_breadth = pack.breadth ? pack.breadth : null;
          // var packages_height = pack.height ? pack.height : null;
          var packages_price = pack.price ? pack.price : null;

          var total_quantity = 0, total_weight = 0, total_price = 0;
          // console.log("packages_quantity---",packages_quantity)
          total_price += (packages_price != '' ? parseFloat(packages_price) : 0);//product_value
          total_quantity += !isNumeric(packages_quantity) ? 1 : packages_quantity;
          total_weight += !isNumeric(packages_actual_weight) ? 0 : packages_actual_weight;
          // console.log("total_price---",total_price)
        }

      }
      var ndr_ticket_created_datetime = data['ndr_ticket']['ndr_ticket_created_datetime'] ? data['ndr_ticket']['ndr_ticket_created_datetime'] : null;

      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var first_ofd_date = sec_ofd_date = third_ofd_date = last_ofd_date = '';
      var i = 0;
      if (tracking != '') {
        // console.log("tracking---",tracking)
        var statuscode = '305';
        var no_of_attempt = 0;
        var tracking_counts = [];
        var obj = {}
        tracking.forEach(function (item) {
          obj[item.status_code] ? obj[item.status_code]++ : obj[item.status_code] = 1;
        });
        // console.log("obj----",obj)  
        // console.log("count---",obj[statuscode])
        no_of_attempt = obj[statuscode];
        // console.log("tracking[0]----",tracking[0])
        var latest_status = '';
        if (status_dispatch_status == '0') {
          latest_status = "PENDING PICKUP";
        } else if (status_dispatch_status == '1') {
          latest_status = "PICKUP DONE";
        } else if (status_dispatch_status == '2') {
          latest_status = "IN-TRANSIT";
        } else if (status_dispatch_status == '4') {
          latest_status = "OUT FOR DELIVERY"
        } else if (status_dispatch_status == '5') {
          latest_status = "DELIVERED"
        } else if (status_dispatch_status == '6') {
          latest_status = "UNDELIVERED"
        } else if (status_dispatch_status == '7') {
          latest_status = "UNDELIVERED"
        } else if (status_dispatch_status == '10') {
          latest_status = "RTO INITIATED"
        } else if (status_dispatch_status == '11') {
          latest_status = "RTO IN-TRANSIT"
        } else if (status_dispatch_status == '12') {
          latest_status = "RTO OUT FOR DELIVERY"
        } else if (status_dispatch_status == '13') {
          latest_status = "RTO DELIVERED"
        } else if (status_dispatch_status == '14') {
          latest_status = "PICKUP CANCEL BY CLIENT"
        } else if (status_dispatch_status == '15') {
          latest_status = "LOST"
        } else if (status_dispatch_status == '16') {
          latest_status = "SHIPMENT DAMAGE"
        } else if (status_dispatch_status == '17') {
          latest_status = "DANGER GOODS"
        } else if (status_dispatch_status == '18') {
          latest_status = "REATTEMPT"
        } else if (status_dispatch_status == '19') {
          latest_status = "RTO"
        } else if (status_dispatch_status == '20') {
          latest_status = "RTO"
        } else if (status_dispatch_status == '21') {
          latest_status = "SELF COLLECT (CS)"
        } else if (status_dispatch_status == '55') {
          latest_status = "RTO UNDELIVERED"
        } else if (status_dispatch_status == '56') {
          latest_status = "REVERSE UNDELIVERED"
        } else if (status_dispatch_status == '1001') {
          latest_status = "UNATTEMPTED"
        } else if (status_dispatch_status == '8') {
          latest_status = "UNDELIVERED"
        } else {
          latest_status = "PENDING PICKUP";
          // status_remark="PENDING PICKUP";
        }
        var ofd_cnt = "0";
        var ndr_sd_remarks = "";
        var ndr_sd = "";
        for await (let track of tracking) {
          // var track_lsp_status_code = track.lsp_status_code ? track.lsp_status_code : null;
          var track_status = track.status ? track.status : null;
          var track_status_code = track.status_code ? track.status_code : null;
          // var track_lsp_status = track.lsp_status ? track.lsp_status : null;
          var track_parent_status_code = track.parent_status_code ? track.parent_status_code : null;
          // var track_source_status_code = track.source_status_code ? track.source_status_code : null;
          var track_remarks = track.remarks ? track.remarks : null;
          var track_updated_date = track.updated_date ? track.updated_date : null;
          var track_location = track.location ? track.location : null;
          // var track_manualentry = track.manualentry ? track.manualentry : null;
          // console.log("track_status_code.length----",track_status_code.length)

          // console.log("track_status_code----",track_status_code)
          // if (track_parent_status_code == "4") {
          //   var last_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
          // } else {
          //   var last_ofd_date = '';
          // }

          if (track_status_code == '305') {
            if (i == 0) {
              var first_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("first_ofd_date----",first_ofd_date)
            } else if (i == 1) {
              var sec_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("sec_ofd_date----",sec_ofd_date)
            } else if (i == 2) {
              var third_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("third_ofd_date----",third_ofd_date)
            }

            var last_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
            var openNDR_curr = new Date();
            var openNDR_ofd = new Date(last_ofd_date);
            const diffTime = Math.abs(openNDR_ofd - openNDR_curr);
            const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
            // console.log(diffTime + " milliseconds");
            var open_ndr_tat = diffDays
            i++;
            // console.log("i---",i)
          } else {
            var first_ofd_date = first_ofd_date;
            var sec_ofd_date = sec_ofd_date;
            var third_ofd_date = third_ofd_date;
            var last_ofd_date = last_ofd_date;
          }


          if (track_parent_status_code == "10" || track_parent_status_code == "18") {
            // var last = [];
            // $removed = array_pop($udx['attributes']['tracking']);
            // var result1 = tracking;
            // last['remarks'] = end(result1);
            var ndr_reason_for_reatttempt = track_remarks;
          }

          if (track_status_code != '' && (track_status_code == "I002" || track_status_code == "I003" || track_status_code == "I004" || track_status_code == "I005" || track_status_code == "I006" || track_status_code == "I007")) {
            // $latest_status = 'UNDELIVERED';
            var ndr_action = track_status;
            var ndr_action_date = moment(track_updated_date).format('YYYY-MM-DD');
          }
          var ndr_action_date = moment(track_updated_date).format('YYYY-MM-DD');
          const date1 = new Date();
          const date2 = new Date(ndr_action_date);
          const diffTime = Math.abs(date2 - date1);
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          // console.log(diffTime + " milliseconds");
          var actionTAT = diffDays
          // console.log("deliverytat@@---",deliverytat + " days");

          const masters = [
            { dispatch_status_code: 99, status: "PENDING PICKUP", remarks: "pending pickup", dispatch_status: 0 },
            { dispatch_status_code: 100, status: "PICKUP DONE", remarks: "pickup done", dispatch_status: 1 },
            { dispatch_status_code: 102, status: "IN-TRANSIT", remarks: "processing at origin hub", dispatch_status: 2 },
            { dispatch_status_code: 305, status: "OUT FOR DELIVERY", remarks: "out for delivery", dispatch_status: 4 },
            { dispatch_status_code: 400, status: "Delivered", remarks: "delivered", dispatch_status: 5 },
            { dispatch_status_code: 401, status: "RTO Delivered", remarks: "rto delivered", dispatch_status: 13 },
            { dispatch_status_code: 500, status: "UNDELIVERED", remarks: "consignee refused", dispatch_status: 6 },
            { dispatch_status_code: 501, status: "UNDELIVERED", remarks: "incomplete address", dispatch_status: 6 },
            { dispatch_status_code: 503, status: "UNDELIVERED", remarks: "oda", dispatch_status: 6 },
            { dispatch_status_code: 504, status: "UNDELIVERED", remarks: "consignee shifted", dispatch_status: 6 },
            { dispatch_status_code: 505, status: "UNDELIVERED", remarks: "DAMAGED", dispatch_status: 6 },
            { dispatch_status_code: 506, status: "UNDELIVERED", remarks: "no such consignee", dispatch_status: 6 },
            { dispatch_status_code: 507, status: "UNDELIVERED", remarks: "future delivery", dispatch_status: 6 },
            { dispatch_status_code: 508, status: "UNDELIVERED", remarks: "cod not ready", dispatch_status: 6 },
            { dispatch_status_code: 509, status: "UNDELIVERED", remarks: "residence/office closed", dispatch_status: 6 },
            { dispatch_status_code: 510, status: "UNDELIVERED", remarks: "out of station", dispatch_status: 6 },
            { dispatch_status_code: 511, status: "UNDELIVERED", remarks: "shipment lost", dispatch_status: 6 },
            { dispatch_status_code: 512, status: "UNDELIVERED", remarks: "Dangerous Goods", dispatch_status: 6 },
            { dispatch_status_code: 513, status: "UNDELIVERED", remarks: "Self Collect", dispatch_status: 6 },
            { dispatch_status_code: 514, status: "UNDELIVERED", remarks: "Held With Govt Authority", dispatch_status: 6 },
            { dispatch_status_code: 515, status: "UNDELIVERED", remarks: "consignee not available", dispatch_status: 6 },
            { dispatch_status_code: 516, status: "UNDELIVERED", remarks: "consignee not responding", dispatch_status: 6 },
            { dispatch_status_code: 517, status: "UNDELIVERED", remarks: "misroute", dispatch_status: 6 },
            { dispatch_status_code: 518, status: "UNDELIVERED", remarks: "on hold", dispatch_status: 6 },
            { dispatch_status_code: 519, status: "UNDELIVERED", remarks: "restricted area", dispatch_status: 6 },
            { dispatch_status_code: 520, status: "UNDELIVERED", remarks: "snatched by consignee", dispatch_status: 6 },
            { dispatch_status_code: 521, status: "UNDELIVERED", remarks: "disturbance/natural disaster/strike/COVID", dispatch_status: 6 },
            { dispatch_status_code: 522, status: "UNDELIVERED", remarks: "Open Delivery", dispatch_status: 6 },
            { dispatch_status_code: 523, status: "UNDELIVERED", remarks: "Customer denied - OTP Delivery", dispatch_status: 6 },
            { dispatch_status_code: 524, status: "UNDELIVERED", remarks: "Time Constraint / Dispute", dispatch_status: 6 },
            { dispatch_status_code: 600, status: "RTO INITIATED", remarks: "rto initiated ", dispatch_status: 10 },
            { dispatch_status_code: 601, status: "RTO IN-TRANSIT", remarks: "rto intransit ", dispatch_status: 11 },
            { dispatch_status_code: 615, status: "RTO UNDELIVERED ", remarks: "Vendor refused ", dispatch_status: 55 },
            { dispatch_status_code: 900, status: "Pickup Cancelled ", remarks: "Pickup Cancelled by Client ", dispatch_status: 14 },
            { dispatch_status_code: 901, status: "LOST ", remarks: "SHIPMENT LOST ", dispatch_status: 15 },
            { dispatch_status_code: 902, status: "SHIPMENT DAMAGE ", remarks: "SHIPMENT DAMAGE ", dispatch_status: 16 },
            { dispatch_status_code: 951, status: "REATTEMPT ", remarks: "Reattempt ", dispatch_status: 18 },
            { dispatch_status_code: 1001, status: "Unattempted ", remarks: "Unattempted ", dispatch_status: 1001 },
            { dispatch_status_code: "I001", status: "SWC ", remarks: "Shared with Client ", dispatch_status: 7 },
            { dispatch_status_code: "I002", status: "CL-REATTEMPT ", remarks: "Client Reattempt ", dispatch_status: 7 },
            { dispatch_status_code: "I003", status: "CL-RTO-INITIATED ", remarks: "cl rto initiated ", dispatch_status: 7 },
            { dispatch_status_code: "I004", status: "CL-HOLD ", remarks: "Client Hold ", dispatch_status: 7 },
            { dispatch_status_code: "I005", status: "CL-SELFCOLLECT ", remarks: "Client Self Collect ", dispatch_status: 7 },
            { dispatch_status_code: "I006", status: "SD-REATTEMPT ", remarks: "Shipdelight Reattempt ", dispatch_status: 7 },
            { dispatch_status_code: "I007", status: "SD-RTO-INITIATED ", remarks: "sd rto initiated ", dispatch_status: 7 },
            { dispatch_status_code: "I008", status: "SD-SELFCOLLECT ", remarks: "Shipdelight Self Collect ", dispatch_status: 7 },
            { dispatch_status_code: "N001", status: "Whatsapp Calling ", remarks: "Whatsapp Calling ", dispatch_status: 8 },
            { dispatch_status_code: 402, status: "Partial Delivered ", remarks: "Partial Delivered ", dispatch_status: 21 },
            { dispatch_status_code: "R1207", status: "Reverse UNDELIVERED ", remarks: "on hold ", dispatch_status: 6 },
            { dispatch_status_code: "R1208", status: "Reverse UNDELIVERED ", remarks: "vendor not available ", dispatch_status: 6 },
          ]


          if (track_status_code == "305")
            // {
            ofd_cnt++;

          if (track_parent_status_code == '6') {
            var scode = tracking[ofd_cnt]['status_code'];

            var result = masters.find(c => c.dispatch_status == status_dispatch_status && c.dispatch_status_code == status_dispatch_status_code)
            if (result != undefined) {
              // console.log("result---",result.remarks)
              ndr_sd_remarks = result.remarks;
            } else {
              ndr_sd_remarks = '';
            }
            ndr_sd = latest_status;
            var status_remark = track_remarks;

          } else {
            ndr_sd_remarks = '';
            ndr_sd = '';
            var status_remark = 'PENDING PICKUP';
          }
          // }

        }

        var count = 0;
        count = tracking.length;
        var client_reattempt_date, client_comment = ''
        if (count > 0) {
          count = count - 1;
          var status_remark = tracking[count]['remarks'];
          if (status_reattempt_date && status_reattempt_date != null) {
            client_reattempt_date = status_reattempt_date;
          }
          else {
            client_reattempt_date = tracking[count]['client_reattempt_date'];;
          }

          if (status_dispatch_status == 'I002') {
            //$client_comment=$udx['attributes']['tracking'][$tot_cn]['remarks'];
            client_comment = tracking[count]['client_comment'];
          }
          else {
            client_comment = tracking[count]['client_comment'];
          }
        } else {
          var status_remark = 'PENDING PICKUP';
          client_comment = '';
          client_reattempt_date = '';
        }


      }




      if (status_pickup_date) {
        var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
      } else {
        var pickup_date = '';
      }

      if (status_last_attempt_date) {
        var last_attempt_date = moment(status_last_attempt_date).format('YYYY-MM-DD');
      } else {
        var last_attempt_date = '';
      }

      var actionDate = ''
      var ndr_update = '';
      if (status_reattempt_date) {
        actionDate = moment(status_reattempt_date).format('YYYY-MM-DD');
      }
      ndr_update = 'NDR instruction not received';
      var action_status = 'Instruction Pending';

      if (last_ofd_date != '' && ndr_action_date != '') {
        var ofddate = last_ofd_date;
        var actiondate = ndr_action_date;

        if (ofddate > actiondate) {
          action_status = 'Reattempt Done';
          ndr_update = 'NDR instruction followed';
        }
        if (last_ofd_date == ndr_action_date) {
          action_status = 'Reattempt Open';
        }

        //NDR Update
        if (actiondate > ofddate) {
          ndr_update = 'Reattempt pending';
        }
      } else {
        var ofddate = '';
        var actiondate = '';
      }

      if (last_ofd_date != '' && ndr_action_date == '') {
        action_status = 'Instruction Pending';
        ndr_update = 'NDR instruction not received';
      }
      if (ndr_action_date != '' && last_ofd_date == '') {
        // echo "in esleif";
        action_status = 'Reattempt Open';
      }

      var order_type = "FORWARD";
      //  console.log("package_rp---",package_rp)
      // if (package_rp && package_rp == 1) {
      //   var order_type = 'REVERSE';
      //   // console.log("order_type---",order_type)
      // }
      if (status_dispatch_status == '6' || (status_dispatch_status_code != 'I003' || status_dispatch_status_code != 'I007')) {
        await ndrReport.push({
          sync_source: sync_source,
          order_type: order_type,
          courier_code: courier_code,
          orderno: orderno,
          airwaybilno: airwaybilno,
          consignorname: consignorname,
          consignorsub_vendorname: consignorsub_vendorname,
          consigneefirstname: consigneefirstname,
          consigneeaddress1: consigneeaddress1,
          consigneeaddress2: consigneeaddress2,
          consigneecity: consigneecity,
          consigneestate: consigneestate,
          consigneepincode: consigneepincode,
          consigneetelephone1: consigneetelephone1,
          consigneetelephone2: consigneetelephone2,
          packages_description: packages_description,
          payment_paytype: payment_paytype,
          package_collectable_value: payment_collectable,
          pickup_date: pickup_date,
          client_reattempt_date: client_reattempt_date,
          client_comment: client_comment,
          ndr_sd: ndr_sd,
          status_remark: status_remark,
          ndr_sd_remarks: ndr_sd_remarks,
          last_attempt_date: last_attempt_date,
          last_ofd_date: last_ofd_date,
          no_of_attempt: no_of_attempt,
          first_ofd_date: first_ofd_date,
          sec_ofd_date: sec_ofd_date,
          third_ofd_date: third_ofd_date,
          ndr_action: ndr_action,
          ndr_action_date: ndr_action_date,
          allocate_date: ndr_ticket_created_datetime,
          allocated_to: '',
          action_by: '',
          open_ndr_tat: open_ndr_tat,
          actionTAT: actionTAT,
          action_status: action_status,
          ndr_update: ndr_update,
        })
      }



    }


    var date = moment(new Date()).format('yyyy-MM-DD');
    var hostname = req.headers.host;
    var currentPath = process.cwd();
    var dir = currentPath + '/src/public/NDR_Reports/';
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }
    var fileName = 'NDR_Report_' + startDate + '_' + endDate + '.csv';
    const csvWriter = createCsvWriter({

      // Output csv file name is geek_data
      path: dir + fileName,
      options:
        { flags: 'a' },
      header: [

        // Title of the columns (column_names)
        { id: 'sync_source', title: 'Data Source' },
        { id: 'order_type', title: 'Order type' },
        { id: 'courier_code', title: 'LSP Code' },
        { id: 'airwaybilno', title: 'AWD' },
        { id: 'orderno', title: 'Order No' },
        { id: 'consignorname', title: 'Client Name' },
        { id: 'consignorsub_vendorname', title: 'Sub Vendor Name' },
        { id: 'consigneefirstname', title: 'Consignee Firstname' },
        { id: 'consigneeaddress1', title: 'Consignee Address1' },
        { id: 'consigneecity', title: 'Destination City' },
        { id: 'consigneestate', title: 'State' },
        { id: 'consigneepincode', title: 'Pincode' },
        { id: 'consigneetelephone1', title: 'Consignee Phone1' },
        { id: 'consigneetelephone2', title: 'Consignee Phone2' },
        { id: 'packages_description', title: 'Product Description' },
        { id: 'payment_paytype', title: 'Payment Type' },
        { id: 'package_collectable_value,  ', title: 'COD Collectable Value' },
        { id: 'pickup_date', title: 'Pickup Date' },
        { id: 'client_reattempt_date', title: 'Client Reattempt Date' },
        { id: 'client_comment', title: 'Client Comment' },
        { id: 'ndr_sd', title: 'SD Status' },
        { id: 'status_remark', title: 'Last NDR Reason' },
        { id: 'ndr_sd_remarks', title: 'SD NDR Reason' },
        { id: 'last_attempt_date', title: 'Last Scan Date' },
        { id: 'last_ofd_date', title: 'Last OFD Date' },
        { id: 'no_of_attempt', title: 'No Of Attempt' },
        { id: 'first_ofd_date', title: '1st OFD Date' },
        { id: 'sec_ofd_date', title: '2nd OFD Date' },
        { id: 'third_ofd_date', title: '3rd OFD Date' },
        { id: 'allocate_date', title: 'Allocate Date' },
        { id: 'allocated_to', title: 'Allocated to' },
        { id: 'action_by', title: 'Action by' },
        { id: 'ndr_action', title: 'NDR Action' },
        { id: 'ndr_action_date', title: 'Action Date' },
        { id: 'open_ndr_tat', title: 'Open NDR TAT' },
        { id: 'actionTAT', title: 'Action TAT' },
        { id: 'action_status', title: 'Action Status' },
        { id: 'ndr_update', title: 'NDR Update' },
      ]
    });
    csvWriter
      .writeRecords(ndrReport)
      .then(() => console.log('Data uploaded into csv successfully'));
    res.json({
      status: 200,
      data: 'http://' + hostname + '/NDR_Reports/' + fileName
    })
  } catch (error) {
    console.log(`Error: ${error.message}`)
    return res.status(400).json({
      message: 'Bad Request',
    })
  }

})

/*
    Get RTO Initiated report using date range (rto_initiated_date)   (Date: 09-02-2022)
*/

router.get('/getRtoInitiatedReport/:startDate/:endDate', async (req, res) => {
  var startDate = trim(req.params.startDate)
  var endDate = trim(req.params.endDate)
  const rtoInitiated = [];
  try {
    const OrdersV1 = await ordersV1Model.find({
      "status.rto_initiated_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      "status.dispatch_status": "10"
    });
    console.log("OrdersV1-----", OrdersV1.length)
    for await (let data of OrdersV1) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'Instashipin V2';
      var transaction_id = data.transaction_id ? data.transaction_id : null;
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        // console.log("consignorname---",consignorname)
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;

        if (status_rto_initiated_date) {
          var rto_initiated_date = moment(status_rto_initiated_date).format('YYYY-MM-DD');
        } else {
          var rto_initiated_date = '';
        }

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        if (status_import_date) {
          var import_date = moment(status_import_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }
        var currentDate = moment(new Date()).format('YYYY-MM-DD');
        if (rto_initiated_date != "" && rto_initiated_date >= '2019-01-01' && currentDate != "" && currentDate >= '2019-01-01') {
          const date1 = new Date();
          // console.log("date1----",date1)
          const date2 = new Date(status_rto_initiated_date);
          const diffTime = Math.abs(date2 - date1);
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          // console.log(diffTime + " milliseconds");
          var deliverytat = diffDays;
          // console.log("deliverytat@@---",deliverytat);
        } else {
          var deliverytat = '';
        }

        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
        } else {
          var aging = '';
        }
      }
      if (data.package) {
        var package_actual_weight = data.package.actual_weight ? data.package.actual_weight : null;
        var package_item_description = data.package.item_description ? data.package.item_description : null;
        var package_items = data.package.items ? data.package.items : null;
        var package_pieces = data.package.pieces ? data.package.pieces : null;
        var package_product = data.package.product ? data.package.product : null;
        var product_value = data.package.product_value ? data.package.product_value : null;
        var package_rp = data.package.rp ? data.package.rp : null;
        var collectable_value = data.package.collectable_value ? data.package.collectable_value : null;

      }

      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var count = 0;
      count = tracking.length;
      if (count > 0) {
        count = count - 1;
        var status_remark = tracking[count]['remarks'];
      } else {
        var status_remark = 'PENDING PICKUP';
      }
      // console.log("status_remark---",status_remark)

      var order_type = "FORWARD";
      //  console.log("package_rp---",package_rp)
      if (package_rp && package_rp == 1) {
        var order_type = 'REVERSE';
        // console.log("order_type---",order_type)
      }

      await rtoInitiated.push({
        airwaybilno: airwaybilno,
        sync_source: sync_source,
        import_date: import_date,
        pickup_date: pickup_date,
        rto_initiated_date: rto_initiated_date,
        transaction_id: transaction_id,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneelastname: consigneelastname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        packages_description: package_item_description,
        packages_quantity: package_pieces,
        product_value: product_value,
        payment_paytype: package_product,
        collectable_value: collectable_value,
        order_type: order_type,
        delivery_tat: deliverytat,
        aging: aging,
        latest_status: "RTO Initiated",
      })



    }
    const OrdersV2 = await ordersV2Model.find({
      "status.rto_initiated_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      "status.dispatch_status": "10"
    })

    console.log("OrdersV2---", OrdersV2.length)
    for await (let data of OrdersV2) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var transaction_id = data.transaction_id ? data.transaction_id : null;
      var sync_source = data.sync_source ? data.sync_source : 'V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;

        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
        var collectable_value = data.payment.collectable ? data.payment.collectable : null;
        var product_value = data.payment.invoice_amount ? data.payment.invoice_amount : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_rp = data.status.rp ? data.status.rp : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;

        if (status_rto_initiated_date) {
          var rto_initiated_date = moment(status_rto_initiated_date).format('YYYY-MM-DD');
        } else {
          var rto_initiated_date = '';
        }

        if (status_booking_date) {
          var import_date = moment(status_booking_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        var currentDate = moment(new Date()).format('YYYY-MM-DD');
        if (rto_initiated_date != "" && rto_initiated_date >= '2019-01-01' && currentDate != "" && currentDate >= '2019-01-01') {
          const date1 = new Date();
          // console.log("date1----",date1)
          const date2 = new Date(status_rto_initiated_date);
          const diffTime = Math.abs(date2 - date1);
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          // console.log(diffTime + " milliseconds");
          var deliverytat = diffDays;
          // console.log("deliverytat@@---",deliverytat);
        } else {
          var deliverytat = '';
        }
        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
        } else {
          var aging = '';
        }
      }

      var packages = data.packages ? data.packages : [];
      if (packages != '') {
        for (let pack of packages) {
          var packages_description = pack.description ? pack.description : null;
          var packages_quantity = pack.quantity ? pack.quantity : null;
          var packages_actual_weight = pack.actual_weight ? pack.actual_weight : null;
          var packages_price = pack.price ? pack.price : null;
          var total_quantity = 0, total_weight = 0, total_price = 0;
          // console.log("packages_quantity---",packages_quantity)
          total_price += (packages_price != '' ? parseFloat(packages_price) : 0);//product_value
          total_quantity += !isNumeric(packages_quantity) ? 1 : packages_quantity;
          total_weight += !isNumeric(packages_actual_weight) ? 0 : packages_actual_weight;
          // console.log("total_price---",total_price)
        }

      }

      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var count = 0;
      count = tracking.length;
      if (count > 0) {
        count = count - 1;
        var status_remark = tracking[count]['remarks'];
      } else {
        var status_remark = 'PENDING PICKUP';
      }
      // console.log("status_remark---",status_remark)
      var order_type = "FORWARD";

      await rtoInitiated.push({
        airwaybilno: data.airwaybilno,
        sync_source: sync_source,
        transaction_id: transaction_id,
        import_date: import_date,
        pickup_date: pickup_date,
        rto_initiated_date: rto_initiated_date,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneelastname: consigneelastname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consigneetelephone2: consigneetelephone2,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        order_type: order_type,
        packages_description: packages_description,
        // total_price: total_price,
        product_value: total_price,
        payment_paytype: payment_paytype,
        order_type: order_type,
        delivery_tat: deliverytat,
        aging: aging,
        latest_status: "RTO Initiated",
      })
    }

    var date = moment(new Date()).format('yyyy-MM-DD');
    var hostname = req.headers.host;
    var currentPath = process.cwd();
    var dir = currentPath + '/src/public/RTO_Initiate_Reports/';
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }
    var fileName = 'RTO_Initiate_Report_' + startDate + '_' + endDate + '.csv';
    const csvWriter = await createCsvWriter({

      // Output csv file name is geek_data
      path: dir + fileName,
      options:
        { flags: 'a' },
      header: [

        // Title of the columns (column_names)
        { id: 'transaction_id', title: 'Transaction No' },
        { id: 'order_type', title: 'Order Type' },
        { id: 'airwaybilno', title: 'AirwayBill No' },
        { id: 'orderno', title: 'Order No' },
        { id: 'courier_code', title: 'Courier Name' },
        { id: 'consignorname', title: 'Client Name' },
        { id: 'consigneefirstname', title: 'Consignee First Name' },
        { id: 'consigneelastname', title: 'Consignee Last Name' },
        { id: 'consigneeaddress1', title: 'Consignee Address' },
        { id: 'consigneecity', title: 'Destination' },
        { id: 'consigneestate', title: 'State' },
        { id: 'consigneepincode', title: 'Destination Pincode' },
        { id: 'consigneetelephone1', title: 'Consignee Phone 1' },
        { id: 'packages_description', title: 'Item Description' },
        { id: 'product_value', title: 'Product Amount' },
        { id: 'consignorsub_vendoraddress1', title: 'Pickup Location' },
        { id: 'import_date', title: 'Import Date' },
        { id: 'pickup_date', title: 'Pickup Date' },
        { id: 'rto_initiated_date', title: 'RTO-Initiate Date' },
        { id: 'aging', title: 'RTO Ageing' },
        { id: 'delivery_tat', title: 'TAT (In Days)' },
        { id: 'latest_status', title: 'Status' },
      ]
    });
    csvWriter
      .writeRecords(rtoInitiated)
      .then(() => console.log('Data uploaded into csv successfully'));
    res.json({
      status: 200,
      data: 'http://' + hostname + '/RTO_Initiate_Reports/' + fileName
    })
  } catch (error) {
    console.log(`Error: ${error.message}`)
    return res.status(400).json({
      message: 'Bad Request',
    })
  }
})

/*
    Get RTO Intransited Ageing report using date range (rto_intransit_date)   (Date: 09-02-2022)
*/
router.get('/getRtoIntransitAgeingReport/:startDate/:endDate', async (req, res) => {
  var startDate = trim(req.params.startDate)
  var endDate = trim(req.params.endDate)
  const rtoIntransit = [];
  try {
    const OrdersV1 = await ordersV1Model.find({
      "status.rto_intransit_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      $or: [
        {
          "status.dispatch_status": '11'
        },
        {
          "status.dispatch_status": '55'
        },
      ],
    });
    console.log("OrdersV1-----", OrdersV1.length)
    for await (let data of OrdersV1) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'Instashipin V2';
      var transaction_id = data.transaction_id ? data.transaction_id : null;
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        // console.log("consignorname---",consignorname)
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_rto_intransit_date = data.status.rto_intransit_date ? data.status.rto_intransit_date : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;

        if (status_rto_intransit_date) {
          var rto_intransit_date = moment(status_rto_intransit_date).format('YYYY-MM-DD');
        } else {
          var rto_intransit_date = '';
        }

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        if (status_import_date) {
          var import_date = moment(status_import_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }

        var currentDate = moment(new Date()).format('YYYY-MM-DD');
        if (rto_intransit_date != "" && rto_intransit_date >= '2019-12-01' && currentDate != "" && currentDate >= '2019-12-01') {
          const date1 = new Date();
          // console.log("date1----",date1)
          const date2 = new Date(status_rto_intransit_date);
          const diffTime = Math.abs(date2 - date1);
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          // console.log(diffTime + " milliseconds");
          var deliverytat = diffDays;
          // console.log("deliverytat@@---",deliverytat);
        } else {
          var deliverytat = '';
        }

        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
        } else {
          var aging = '';
        }

      }
      if (data.package) {
        var package_actual_weight = data.package.actual_weight ? data.package.actual_weight : null;
        var package_item_description = data.package.item_description ? data.package.item_description : null;
        var package_items = data.package.items ? data.package.items : null;
        var package_pieces = data.package.pieces ? data.package.pieces : null;
        var package_product = data.package.product ? data.package.product : null;
        var product_value = data.package.product_value ? data.package.product_value : null;
        var package_rp = data.package.rp ? data.package.rp : null;
        var collectable_value = data.package.collectable_value ? data.package.collectable_value : null;

      }

      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var count = 0;
      count = tracking.length;
      if (count > 0) {
        count = count - 1;
        var status_remark = tracking[count]['remarks'];
      } else {
        var status_remark = 'PENDING PICKUP';
      }
      // console.log("status_remark---",status_remark)

      var order_type = "FORWARD";
      //  console.log("package_rp---",package_rp)
      if (package_rp && package_rp == 1) {
        var order_type = 'REVERSE';
        // console.log("order_type---",order_type)
      }
      var latest_status = '';
      if (status_dispatch_status == '11')
        latest_status = "RTO IN-TRANSIT";
      if (status_dispatch_status == '55')
        latest_status = "RTO UNDELIVERED";

      await rtoIntransit.push({
        airwaybilno: airwaybilno,
        sync_source: sync_source,
        import_date: import_date,
        pickup_date: pickup_date,
        rto_intransit_date: rto_intransit_date,
        transaction_id: transaction_id,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneelastname: consigneelastname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        packages_description: package_item_description,
        packages_quantity: package_pieces,
        product_value: product_value,
        payment_paytype: package_product,
        collectable_value: collectable_value,
        order_type: order_type,
        delivery_tat: deliverytat,
        aging: aging,
        latest_status: latest_status,
      })

    }
    const OrdersV2 = await ordersV2Model.find({
      "status.rto_intransit_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      $or: [
        {
          "status.dispatch_status": '11'
        },
        {
          "status.dispatch_status": '55'
        },
      ],
    })

    console.log("OrdersV2---", OrdersV2.length)
    for await (let data of OrdersV2) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var transaction_id = data.transaction_id ? data.transaction_id : null;
      var sync_source = data.sync_source ? data.sync_source : 'V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;

        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
        var collectable_value = data.payment.collectable ? data.payment.collectable : null;
        var product_value = data.payment.invoice_amount ? data.payment.invoice_amount : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_rp = data.status.rp ? data.status.rp : null;
        var status_rto_intransit_date = data.status.rto_intransit_date ? data.status.rto_intransit_date : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;

        if (status_rto_intransit_date) {
          var rto_intransit_date = moment(status_rto_intransit_date).format('YYYY-MM-DD');
        } else {
          var rto_intransit_date = '';
        }

        if (status_booking_date) {
          var import_date = moment(status_booking_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        var currentDate = moment(new Date()).format('YYYY-MM-DD');
        if (rto_intransit_date != "" && rto_intransit_date >= '2019-12-01' && currentDate != "" && currentDate >= '2019-12-01') {
          const date1 = new Date();
          // console.log("date1----",date1)
          const date2 = new Date(status_rto_intransit_date);
          const diffTime = Math.abs(date2 - date1);
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          // console.log(diffTime + " milliseconds");
          var deliverytat = diffDays;
          // console.log("deliverytat@@---",deliverytat);
        } else {
          var deliverytat = '';
        }

        if (deliverytat == "0") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "1") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "2") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "3") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "4") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "5") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "6") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
        } else {
          var aging = '';
        }
      }

      var packages = data.packages ? data.packages : [];
      if (packages != '') {
        for (let pack of packages) {
          var packages_description = pack.description ? pack.description : null;
          var packages_quantity = pack.quantity ? pack.quantity : null;
          var packages_actual_weight = pack.actual_weight ? pack.actual_weight : null;
          var packages_price = pack.price ? pack.price : null;
          var total_quantity = 0, total_weight = 0, total_price = 0;
          // console.log("packages_quantity---",packages_quantity)
          total_price += (packages_price != '' ? parseFloat(packages_price) : 0);//product_value
          total_quantity += !isNumeric(packages_quantity) ? 1 : packages_quantity;
          total_weight += !isNumeric(packages_actual_weight) ? 0 : packages_actual_weight;
          // console.log("total_price---",total_price)
        }

      }

      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var count = 0;
      count = tracking.length;
      if (count > 0) {
        count = count - 1;
        var status_remark = tracking[count]['remarks'];
      } else {
        var status_remark = 'PENDING PICKUP';
      }
      // console.log("status_remark---",status_remark)
      var latest_status = '';
      if (status_dispatch_status == '11')
        latest_status = "RTO IN-TRANSIT";
      if (status_dispatch_status == '55')
        latest_status = "RTO UNDELIVERED";

      var order_type = "FORWARD";

      await rtoIntransit.push({
        airwaybilno: data.airwaybilno,
        sync_source: sync_source,
        transaction_id: transaction_id,
        import_date: import_date,
        pickup_date: pickup_date,
        rto_intransit_date: rto_intransit_date,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneelastname: consigneelastname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consigneetelephone2: consigneetelephone2,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        order_type: order_type,
        packages_description: packages_description,
        // total_price: total_price,
        product_value: total_price,
        payment_paytype: payment_paytype,
        order_type: order_type,
        delivery_tat: deliverytat,
        aging: aging,
        latest_status: latest_status,
      })
    }

    var date = moment(new Date()).format('yyyy-MM-DD');
    var hostname = req.headers.host;
    var currentPath = process.cwd();
    var dir = currentPath + '/src/public/RTO_Intransit_Ageing_Reports/';
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }
    var fileName = 'RTO_Intransit_Ageing_Report_' + startDate + '_' + endDate + '.csv';
    const csvWriter = await createCsvWriter({

      // Output csv file name is geek_data
      path: dir + fileName,
      options:
        { flags: 'a' },
      header: [

        // Title of the columns (column_names)
        { id: 'transaction_id', title: 'Transaction No' },
        { id: 'order_type', title: 'Order Type' },
        { id: 'airwaybilno', title: 'AirwayBill No' },
        { id: 'orderno', title: 'Order No' },
        { id: 'courier_code', title: 'Courier Name' },
        { id: 'consignorname', title: 'Client Name' },
        { id: 'consigneefirstname', title: 'Consignee First Name' },
        { id: 'consigneelastname', title: 'Consignee Last Name' },
        { id: 'consigneeaddress1', title: 'Consignee Address' },
        { id: 'consigneecity', title: 'Destination' },
        { id: 'consigneestate', title: 'State' },
        { id: 'consigneepincode', title: 'Destination Pincode' },
        { id: 'consigneetelephone1', title: 'Consignee Phone 1' },
        { id: 'packages_description', title: 'Item Description' },
        { id: 'product_value', title: 'Product Amount' },
        { id: 'consignorsub_vendoraddress1', title: 'Pickup Location' },
        { id: 'import_date', title: 'Import Date' },
        { id: 'pickup_date', title: 'Pickup Date' },
        { id: 'rto_intransit_date', title: 'RTO-Intransit Date' },
        { id: 'aging', title: 'RTO Ageing' },
        { id: 'delivery_tat', title: 'TAT (In Days)' },
        { id: 'latest_status', title: 'Status' },
      ]
    });
    csvWriter
      .writeRecords(rtoIntransit)
      .then(() => console.log('Data uploaded into csv successfully'));
    res.json({
      status: 200,
      data: 'http://' + hostname + '/RTO_Intransit_Ageing_Reports/' + fileName
    })
  } catch (error) {
    console.log(`Error: ${error.message}`)
    return res.status(400).json({
      message: 'Bad Request',
    })
  }
})

/*
    Get OFD Ageing report using date range (last_attempt_date)   (Date: 09-02-2022)
*/
router.get('/getOFDAgeingReport/:startDate/:endDate', async (req, res) => {
  var startDate = trim(req.params.startDate)
  var endDate = trim(req.params.endDate)
  const ofdAgeing = [];
  try {
    const OrdersV1 = await ordersV1Model.find({
      "status.last_attempt_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },

    });
    console.log("OrdersV1-----", OrdersV1.length)
    for await (let data of OrdersV1) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'Instashipin V2';
      var transaction_id = data.transaction_id ? data.transaction_id : null;
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        // console.log("consignorname---",consignorname)
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_last_attempt_date = data.status.last_attempt_date ? data.status.last_attempt_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;

        if (status_rto_initiated_date) {
          var rto_initiated_date = moment(status_rto_initiated_date).format('YYYY-MM-DD');
        } else {
          var rto_initiated_date = '';
        }
        if (status_last_attempt_date) {
          var last_attempt_date = moment(status_last_attempt_date).format('YYYY-MM-DD');
        } else {
          var last_attempt_date = '';
        }

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        if (status_import_date) {
          var import_date = moment(status_import_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }
        var currentDate = moment(new Date()).format('YYYY-MM-DD');
        if (last_attempt_date != "" && last_attempt_date >= '2019-01-01' && currentDate != "" && currentDate >= '2019-01-01') {
          const date1 = new Date();
          // console.log("date1----",date1)
          const date2 = new Date(status_last_attempt_date);
          const diffTime = Math.abs(date2 - date1);
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          // console.log(diffTime + " milliseconds");
          var deliverytat = diffDays;
          // console.log("deliverytat@@---",deliverytat);
        } else {
          var deliverytat = '';
        }

        if (deliverytat == "0") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "1") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "2") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "3") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "4") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "5") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "6") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
        } else {
          var aging = '';
        }

      }
      if (data.package) {
        var package_actual_weight = data.package.actual_weight ? data.package.actual_weight : null;
        var package_item_description = data.package.item_description ? data.package.item_description : null;
        var package_items = data.package.items ? data.package.items : null;
        var package_pieces = data.package.pieces ? data.package.pieces : null;
        var package_product = data.package.product ? data.package.product : null;
        var product_value = data.package.product_value ? data.package.product_value : null;
        var package_rp = data.package.rp ? data.package.rp : null;
        var collectable_value = data.package.collectable_value ? data.package.collectable_value : null;

      }

      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var count = 0;
      count = tracking.length;
      if (count > 0) {
        count = count - 1;
        var status_remark = tracking[count]['remarks'];
      } else {
        var status_remark = 'PENDING PICKUP';
      }
      // console.log("status_remark---",status_remark)

      var order_type = "FORWARD";
      //  console.log("package_rp---",package_rp)
      if (package_rp && package_rp == 1) {
        var order_type = 'REVERSE';
        // console.log("order_type---",order_type)
      }

      await ofdAgeing.push({
        airwaybilno: airwaybilno,
        sync_source: sync_source,
        import_date: import_date,
        pickup_date: pickup_date,
        last_attempt_date: last_attempt_date,
        transaction_id: transaction_id,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneelastname: consigneelastname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        packages_description: package_item_description,
        packages_quantity: package_pieces,
        product_value: product_value,
        payment_paytype: package_product,
        collectable_value: collectable_value,
        order_type: order_type,
        delivery_tat: deliverytat,
        aging: aging,
        latest_status: "OFD",
      })


    }
    const OrdersV2 = await ordersV2Model.find({
      "status.last_attempt_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },

    })

    console.log("OrdersV2---", OrdersV2.length)
    for await (let data of OrdersV2) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var transaction_id = data.transaction_id ? data.transaction_id : null;
      var sync_source = data.sync_source ? data.sync_source : 'V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;

        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
        var collectable_value = data.payment.collectable ? data.payment.collectable : null;
        var product_value = data.payment.invoice_amount ? data.payment.invoice_amount : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_rp = data.status.rp ? data.status.rp : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_last_attempt_date = data.status.last_attempt_date ? data.status.last_attempt_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;

        if (status_rto_initiated_date) {
          var rto_initiated_date = moment(status_rto_initiated_date).format('YYYY-MM-DD');
        } else {
          var rto_initiated_date = '';
        }

        if (status_last_attempt_date) {
          var last_attempt_date = moment(status_last_attempt_date).format('YYYY-MM-DD');
        } else {
          var last_attempt_date = '';
        }

        if (status_booking_date) {
          var import_date = moment(status_booking_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }
        var currentDate = moment(new Date()).format('YYYY-MM-DD');
        if (last_attempt_date != "" && last_attempt_date >= '2019-01-01' && currentDate != "" && currentDate >= '2019-01-01') {
          const date1 = new Date();
          // console.log("date1----",date1)
          const date2 = new Date(status_last_attempt_date);
          const diffTime = Math.abs(date2 - date1);
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          // console.log(diffTime + " milliseconds");
          var deliverytat = diffDays;
          // console.log("deliverytat@@---",deliverytat);
        } else {
          var deliverytat = '';
        }

        if (deliverytat == "0") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "1") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "2") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "3") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "4") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "5") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "6") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
        } else {
          var aging = '';
        }
      }

      var packages = data.packages ? data.packages : [];
      if (packages != '') {
        for (let pack of packages) {
          var packages_description = pack.description ? pack.description : null;
          var packages_quantity = pack.quantity ? pack.quantity : null;
          var packages_actual_weight = pack.actual_weight ? pack.actual_weight : null;
          var packages_price = pack.price ? pack.price : null;
          var total_quantity = 0, total_weight = 0, total_price = 0;
          // console.log("packages_quantity---",packages_quantity)
          total_price += (packages_price != '' ? parseFloat(packages_price) : 0);//product_value
          total_quantity += !isNumeric(packages_quantity) ? 1 : packages_quantity;
          total_weight += !isNumeric(packages_actual_weight) ? 0 : packages_actual_weight;
          // console.log("total_price---",total_price)
        }

      }

      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var count = 0;
      count = tracking.length;
      if (count > 0) {
        count = count - 1;
        var status_remark = tracking[count]['remarks'];
      } else {
        var status_remark = 'PENDING PICKUP';
      }
      // console.log("status_remark---",status_remark)
      var order_type = "FORWARD";

      await ofdAgeing.push({
        airwaybilno: data.airwaybilno,
        sync_source: sync_source,
        transaction_id: transaction_id,
        import_date: import_date,
        pickup_date: pickup_date,
        last_attempt_date: last_attempt_date,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneelastname: consigneelastname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consigneetelephone2: consigneetelephone2,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        order_type: order_type,
        packages_description: packages_description,
        // total_price: total_price,
        product_value: total_price,
        payment_paytype: payment_paytype,
        order_type: order_type,
        delivery_tat: deliverytat,
        aging: aging,
        latest_status: "OFD",
      })
    }

    var date = moment(new Date()).format('yyyy-MM-DD');
    var hostname = req.headers.host;
    var currentPath = process.cwd();
    var dir = currentPath + '/src/public/OFD_Ageing_Reports/';
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }
    var fileName = 'OFD_Ageing_Report_' + startDate + '_' + endDate + '.csv';
    const csvWriter = await createCsvWriter({

      // Output csv file name is geek_data
      path: dir + fileName,
      options:
        { flags: 'a' },
      header: [

        // Title of the columns (column_names)
        { id: 'transaction_id', title: 'Transaction No' },
        { id: 'order_type', title: 'Order Type' },
        { id: 'airwaybilno', title: 'AirwayBill No' },
        { id: 'orderno', title: 'Order No' },
        { id: 'courier_code', title: 'Courier Name' },
        { id: 'consignorname', title: 'Client Name' },
        { id: 'consigneefirstname', title: 'Consignee First Name' },
        { id: 'consigneelastname', title: 'Consignee Last Name' },
        { id: 'consigneeaddress1', title: 'Consignee Address' },
        { id: 'consigneecity', title: 'Destination' },
        { id: 'consigneestate', title: 'State' },
        { id: 'consigneepincode', title: 'Destination Pincode' },
        { id: 'consigneetelephone1', title: 'Consignee Phone 1' },
        { id: 'packages_description', title: 'Item Description' },
        { id: 'product_value', title: 'Product Amount' },
        { id: 'consignorsub_vendoraddress1', title: 'Pickup Location' },
        { id: 'import_date', title: 'Import Date' },
        { id: 'pickup_date', title: 'Pickup Date' },
        { id: 'last_attempt_date', title: 'OFD Date' },
        { id: 'aging', title: 'OFD Ageing' },
        { id: 'delivery_tat', title: 'TAT (In Days)' },
        { id: 'latest_status', title: 'Status' },
      ]
    });
    csvWriter
      .writeRecords(ofdAgeing)
      .then(() => console.log('Data uploaded into csv successfully'));
    res.json({
      status: 200,
      data: 'http://' + hostname + '/OFD_Ageing_Reports/' + fileName
    })
  } catch (error) {
    console.log(`Error: ${error.message}`)
    return res.status(400).json({
      message: 'Bad Request',
    })
  }
})

/*
    Get Intransit Ageing report using date range (import_date and booking_date)   (Date: 09-02-2022)
*/
router.get('/getIntransitAgeingReport/:startDate/:endDate', async (req, res) => {
  var startDate = trim(req.params.startDate)
  var endDate = trim(req.params.endDate)
  const intransitAgeing = [];
  try {
    const OrdersV1 = await ordersV1Model.find({
      "status.pickup_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      "status.dispatch_status": '2'

    });
    console.log("OrdersV1-----", OrdersV1.length)
    for await (let data of OrdersV1) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'Instashipin V2';
      var transaction_id = data.transaction_id ? data.transaction_id : null;
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        // console.log("consignorname---",consignorname)
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_last_attempt_date = data.status.last_attempt_date ? data.status.last_attempt_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;


        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        if (status_import_date) {
          var import_date = moment(status_import_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }
        var currentDate = moment(new Date()).format('YYYY-MM-DD');
        if (pickup_date != "" && pickup_date >= '2019-12-01' && currentDate != "" && currentDate >= '2019-12-01') {
          const date1 = new Date();
          // console.log("date1----",date1)
          const date2 = new Date(status_pickup_date);
          const diffTime = Math.abs(date2 - date1);
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          // console.log(diffTime + " milliseconds");
          var deliverytat = diffDays;
          // console.log("deliverytat@@---",deliverytat);
        } else {
          var deliverytat = '';
        }

        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
        } else {
          var aging = '';
        }

      }
      if (data.package) {
        var package_actual_weight = data.package.actual_weight ? data.package.actual_weight : null;
        var package_item_description = data.package.item_description ? data.package.item_description : null;
        var package_items = data.package.items ? data.package.items : null;
        var package_pieces = data.package.pieces ? data.package.pieces : null;
        var package_product = data.package.product ? data.package.product : null;
        var product_value = data.package.product_value ? data.package.product_value : null;
        var package_rp = data.package.rp ? data.package.rp : null;
        var collectable_value = data.package.collectable_value ? data.package.collectable_value : null;

      }

      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var count = 0;
      count = tracking.length;
      if (count > 0) {
        count = count - 1;
        var status_remark = tracking[count]['remarks'];
      } else {
        var status_remark = 'PENDING PICKUP';
      }
      // console.log("status_remark---",status_remark)

      var order_type = "FORWARD";
      //  console.log("package_rp---",package_rp)
      if (package_rp && package_rp == 1) {
        var order_type = 'REVERSE';
        // console.log("order_type---",order_type)
      }

      await intransitAgeing.push({
        airwaybilno: airwaybilno,
        sync_source: sync_source,
        import_date: import_date,
        pickup_date: pickup_date,
        transaction_id: transaction_id,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneelastname: consigneelastname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        packages_description: package_item_description,
        packages_quantity: package_pieces,
        product_value: product_value,
        payment_paytype: package_product,
        collectable_value: collectable_value,
        order_type: order_type,
        delivery_tat: deliverytat,
        aging: aging,
        latest_status: "Intransit",
      })


    }
    const OrdersV2 = await ordersV2Model.find({
      "status.pickup_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      "status.dispatch_status": '2'
    })

    console.log("OrdersV2---", OrdersV2.length)
    for await (let data of OrdersV2) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var transaction_id = data.transaction_id ? data.transaction_id : null;
      var sync_source = data.sync_source ? data.sync_source : 'V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;

        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
        var collectable_value = data.payment.collectable ? data.payment.collectable : null;
        var product_value = data.payment.invoice_amount ? data.payment.invoice_amount : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_rp = data.status.rp ? data.status.rp : null;
        var status_rto_intransit_date = data.status.rto_intransit_date ? data.status.rto_intransit_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_last_attempt_date = data.status.last_attempt_date ? data.status.last_attempt_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;


        if (status_booking_date) {
          var import_date = moment(status_booking_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        var currentDate = moment(new Date()).format('YYYY-MM-DD');
        if (pickup_date != "" && pickup_date >= '2019-12-01' && currentDate != "" && currentDate >= '2019-12-01') {
          const date1 = new Date();
          // console.log("date1----",date1)
          const date2 = new Date(status_pickup_date);
          const diffTime = Math.abs(date2 - date1);
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          // console.log(diffTime + " milliseconds");
          var deliverytat = diffDays;
          // console.log("deliverytat@@---",deliverytat);
        } else {
          var deliverytat = '';
        }

        if (deliverytat == "0") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "1") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "2") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "3") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "4") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "5") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat == "6") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
        } else {
          var aging = '';
        }
      }

      var packages = data.packages ? data.packages : [];
      if (packages != '') {
        for (let pack of packages) {
          var packages_description = pack.description ? pack.description : null;
          var packages_quantity = pack.quantity ? pack.quantity : null;
          var packages_actual_weight = pack.actual_weight ? pack.actual_weight : null;
          var packages_price = pack.price ? pack.price : null;
          var total_quantity = 0, total_weight = 0, total_price = 0;
          // console.log("packages_quantity---",packages_quantity)
          total_price += (packages_price != '' ? parseFloat(packages_price) : 0);//product_value
          total_quantity += !isNumeric(packages_quantity) ? 1 : packages_quantity;
          total_weight += !isNumeric(packages_actual_weight) ? 0 : packages_actual_weight;
          // console.log("total_price---",total_price)
        }

      }

      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var count = 0;
      count = tracking.length;
      if (count > 0) {
        count = count - 1;
        var status_remark = tracking[count]['remarks'];
      } else {
        var status_remark = 'PENDING PICKUP';
      }
      // console.log("status_remark---",status_remark)
      var order_type = "FORWARD";

      await intransitAgeing.push({
        airwaybilno: data.airwaybilno,
        sync_source: sync_source,
        transaction_id: transaction_id,
        import_date: import_date,
        pickup_date: pickup_date,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneelastname: consigneelastname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consigneetelephone2: consigneetelephone2,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        order_type: order_type,
        packages_description: packages_description,
        // total_price: total_price,
        product_value: total_price,
        payment_paytype: payment_paytype,
        order_type: order_type,
        delivery_tat: deliverytat,
        aging: aging,
        latest_status: "Intransit",
      })
    }

    var date = moment(new Date()).format('yyyy-MM-DD');
    var hostname = req.headers.host;
    var currentPath = process.cwd();
    var dir = currentPath + '/src/public/Intransit_Ageing_Reports/';
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }
    var fileName = 'Intransit_Ageing_Report_' + startDate + '_' + endDate + '.csv';
    const csvWriter = await createCsvWriter({

      // Output csv file name is geek_data
      path: dir + fileName,
      options:
        { flags: 'a' },
      header: [

        // Title of the columns (column_names)
        { id: 'transaction_id', title: 'Transaction No' },
        { id: 'order_type', title: 'Order Type' },
        { id: 'airwaybilno', title: 'AirwayBill No' },
        { id: 'orderno', title: 'Order No' },
        { id: 'courier_code', title: 'Courier Name' },
        { id: 'consignorname', title: 'Client Name' },
        { id: 'consigneefirstname', title: 'Consignee First Name' },
        { id: 'consigneelastname', title: 'Consignee Last Name' },
        { id: 'consigneeaddress1', title: 'Consignee Address' },
        { id: 'consigneecity', title: 'Destination' },
        { id: 'consigneestate', title: 'State' },
        { id: 'consigneepincode', title: 'Destination Pincode' },
        { id: 'consigneetelephone1', title: 'Consignee Phone 1' },
        { id: 'packages_description', title: 'Item Description' },
        { id: 'product_value', title: 'Product Amount' },
        { id: 'consignorsub_vendoraddress1', title: 'Pickup Location' },
        { id: 'import_date', title: 'Import Date' },
        { id: 'pickup_date', title: 'Pickup Date' },
        { id: 'pickup_date', title: 'Intransit Date' },
        { id: 'aging', title: 'Intransit Ageing' },
        { id: 'delivery_tat', title: 'TAT (In Days)' },
        { id: 'latest_status', title: 'Status' },
      ]
    });
    csvWriter
      .writeRecords(intransitAgeing)
      .then(() => console.log('Data uploaded into csv successfully'));
    res.json({
      status: 200,
      data: 'http://' + hostname + '/Intransit_Ageing_Reports/' + fileName
    })
  } catch (error) {
    console.log(`Error: ${error.message}`)
    return res.status(400).json({
      message: 'Bad Request',
    })
  }
})

/*
    Get RTO Initiated Ageing report using date range (rto_initiated_date)   (Date: 10-02-2022)
*/
router.get('/getRtoInitiatedAgeingReport/:startDate/:endDate', async (req, res) => {
  var startDate = trim(req.params.startDate)
  var endDate = trim(req.params.endDate)
  const rtoInitiatedAgeing = [];
  try {
    const OrdersV1 = await ordersV1Model.find({
      "status.rto_initiated_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      }
    });
    console.log("OrdersV1-----", OrdersV1.length)
    for await (let data of OrdersV1) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'Instashipin V2';
      var transaction_id = data.transaction_id ? data.transaction_id : null;
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        // console.log("consignorname---",consignorname)
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;

        if (status_rto_initiated_date) {
          var rto_initiated_date = moment(status_rto_initiated_date).format('YYYY-MM-DD');
        } else {
          var rto_initiated_date = '';
        }

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        if (status_import_date) {
          var import_date = moment(status_import_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }
        var currentDate = moment(new Date()).format('YYYY-MM-DD');
        if (rto_initiated_date != "" && rto_initiated_date >= '2019-12-01' && currentDate != "" && currentDate >= '2019-12-01') {
          const date1 = new Date();
          // console.log("date1----",date1)
          const date2 = new Date(status_rto_initiated_date);
          const diffTime = Math.abs(date2 - date1);
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          // console.log(diffTime + " milliseconds");
          var deliverytat = diffDays;
          // console.log("deliverytat@@---",deliverytat);
        } else {
          var deliverytat = '';
        }

        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
        } else {
          var aging = '';
        }
      }
      if (data.package) {
        var package_actual_weight = data.package.actual_weight ? data.package.actual_weight : null;
        var package_item_description = data.package.item_description ? data.package.item_description : null;
        var package_items = data.package.items ? data.package.items : null;
        var package_pieces = data.package.pieces ? data.package.pieces : null;
        var package_product = data.package.product ? data.package.product : null;
        var product_value = data.package.product_value ? data.package.product_value : null;
        var package_rp = data.package.rp ? data.package.rp : null;
        var collectable_value = data.package.collectable_value ? data.package.collectable_value : null;

      }

      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var count = 0;
      count = tracking.length;
      if (count > 0) {
        count = count - 1;
        var status_remark = tracking[count]['remarks'];
      } else {
        var status_remark = 'PENDING PICKUP';
      }
      // console.log("status_remark---",status_remark)

      var order_type = "FORWARD";
      //  console.log("package_rp---",package_rp)
      if (package_rp && package_rp == 1) {
        var order_type = 'REVERSE';
        // console.log("order_type---",order_type)
      }
      if (status_dispatch_status == '10' || (status_dispatch_status_code == 'I003' || status_dispatch_status_code == 'I007')) {
        await rtoInitiatedAgeing.push({
          airwaybilno: airwaybilno,
          sync_source: sync_source,
          import_date: import_date,
          pickup_date: pickup_date,
          rto_initiated_date: rto_initiated_date,
          transaction_id: transaction_id,
          consigneeaddress1: consigneeaddress1,
          consigneeaddress2: consigneeaddress2,
          consigneecity: consigneecity,
          consigneefirstname: consigneefirstname,
          consigneelastname: consigneelastname,
          consigneepincode: consigneepincode,
          consigneestate: consigneestate,
          consigneetelephone1: consigneetelephone1,
          consignorname: consignorname,
          consignorsub_vendoraddress1: consignorsub_vendoraddress1,
          consignorsub_vendoraddress2: consignorsub_vendoraddress2,
          consignorsub_vendorcity: consignorsub_vendorcity,
          consignorsub_vendorname: consignorsub_vendorname,
          consignorsub_vendorphone: consignorsub_vendorphone,
          consignorsub_vendorpincode: consignorsub_vendorpincode,
          consignorsub_vendorstate: consignorsub_vendorstate,
          courier_code: courier_code,
          orderno: orderno,
          packages_description: package_item_description,
          packages_quantity: package_pieces,
          product_value: product_value,
          payment_paytype: package_product,
          collectable_value: collectable_value,
          order_type: order_type,
          delivery_tat: deliverytat,
          aging: aging,
          latest_status: "RTO Initiated",
        })
      }

    }
    const OrdersV2 = await ordersV2Model.find({
      "status.rto_initiated_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      }
    })

    console.log("OrdersV2---", OrdersV2.length)
    for await (let data of OrdersV2) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var transaction_id = data.transaction_id ? data.transaction_id : null;
      var sync_source = data.sync_source ? data.sync_source : 'V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;

        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
        var collectable_value = data.payment.collectable ? data.payment.collectable : null;
        var product_value = data.payment.invoice_amount ? data.payment.invoice_amount : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_rp = data.status.rp ? data.status.rp : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;

        if (status_rto_initiated_date) {
          var rto_initiated_date = moment(status_rto_initiated_date).format('YYYY-MM-DD');
        } else {
          var rto_initiated_date = '';
        }

        if (status_booking_date) {
          var import_date = moment(status_booking_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        var currentDate = moment(new Date()).format('YYYY-MM-DD');
        if (rto_initiated_date != "" && rto_initiated_date >= '2019-12-01' && currentDate != "" && currentDate >= '2019-12-01') {
          const date1 = new Date();
          // console.log("date1----",date1)
          const date2 = new Date(status_rto_initiated_date);
          const diffTime = Math.abs(date2 - date1);
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          // console.log(diffTime + " milliseconds");
          var deliverytat = diffDays;
          // console.log("deliverytat@@---",deliverytat);
        } else {
          var deliverytat = '';
        }
        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
        } else {
          var aging = '';
        }
      }

      var packages = data.packages ? data.packages : [];
      if (packages != '') {
        for (let pack of packages) {
          var packages_description = pack.description ? pack.description : null;
          var packages_quantity = pack.quantity ? pack.quantity : null;
          var packages_actual_weight = pack.actual_weight ? pack.actual_weight : null;
          var packages_price = pack.price ? pack.price : null;
          var total_quantity = 0, total_weight = 0, total_price = 0;
          // console.log("packages_quantity---",packages_quantity)
          total_price += (packages_price != '' ? parseFloat(packages_price) : 0);//product_value
          total_quantity += !isNumeric(packages_quantity) ? 1 : packages_quantity;
          total_weight += !isNumeric(packages_actual_weight) ? 0 : packages_actual_weight;
          // console.log("total_price---",total_price)
        }

      }

      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var count = 0;
      count = tracking.length;
      if (count > 0) {
        count = count - 1;
        var status_remark = tracking[count]['remarks'];
      } else {
        var status_remark = 'PENDING PICKUP';
      }
      // console.log("status_remark---",status_remark)
      var order_type = "FORWARD";

      if (status_dispatch_status == '10' || (status_dispatch_status_code == 'I003' || status_dispatch_status_code == 'I007')) {
        await rtoInitiatedAgeing.push({
          airwaybilno: data.airwaybilno,
          sync_source: sync_source,
          transaction_id: transaction_id,
          import_date: import_date,
          pickup_date: pickup_date,
          rto_initiated_date: rto_initiated_date,
          consigneeaddress1: consigneeaddress1,
          consigneeaddress2: consigneeaddress2,
          consigneecity: consigneecity,
          consigneefirstname: consigneefirstname,
          consigneelastname: consigneelastname,
          consigneepincode: consigneepincode,
          consigneestate: consigneestate,
          consigneetelephone1: consigneetelephone1,
          consigneetelephone2: consigneetelephone2,
          consignorname: consignorname,
          consignorsub_vendoraddress1: consignorsub_vendoraddress1,
          consignorsub_vendoraddress2: consignorsub_vendoraddress2,
          consignorsub_vendorcity: consignorsub_vendorcity,
          consignorsub_vendorname: consignorsub_vendorname,
          consignorsub_vendorphone: consignorsub_vendorphone,
          consignorsub_vendorpincode: consignorsub_vendorpincode,
          consignorsub_vendorstate: consignorsub_vendorstate,
          courier_code: courier_code,
          orderno: orderno,
          order_type: order_type,
          packages_description: packages_description,
          // total_price: total_price,
          product_value: total_price,
          payment_paytype: payment_paytype,
          order_type: order_type,
          delivery_tat: deliverytat,
          aging: aging,
          latest_status: "RTO Initiated",
        })
      }

    }

    var date = moment(new Date()).format('yyyy-MM-DD');
    var hostname = req.headers.host;
    var currentPath = process.cwd();
    var dir = currentPath + '/src/public/RTO_Initiate_Ageing_Reports/';
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }
    var fileName = 'RTO_Initiate_Ageing_Report_' + startDate + '_' + endDate + '.csv';
    const csvWriter = await createCsvWriter({

      // Output csv file name is geek_data
      path: dir + fileName,
      options:
        { flags: 'a' },
      header: [

        // Title of the columns (column_names)
        { id: 'transaction_id', title: 'Transaction No' },
        { id: 'order_type', title: 'Order Type' },
        { id: 'airwaybilno', title: 'AirwayBill No' },
        { id: 'orderno', title: 'Order No' },
        { id: 'courier_code', title: 'Courier Name' },
        { id: 'consignorname', title: 'Client Name' },
        { id: 'consigneefirstname', title: 'Consignee First Name' },
        { id: 'consigneelastname', title: 'Consignee Last Name' },
        { id: 'consigneeaddress1', title: 'Consignee Address' },
        { id: 'consigneecity', title: 'Destination' },
        { id: 'consigneestate', title: 'State' },
        { id: 'consigneepincode', title: 'Destination Pincode' },
        { id: 'consigneetelephone1', title: 'Consignee Phone 1' },
        { id: 'packages_description', title: 'Item Description' },
        { id: 'product_value', title: 'Product Amount' },
        { id: 'consignorsub_vendoraddress1', title: 'Pickup Location' },
        { id: 'import_date', title: 'Import Date' },
        { id: 'pickup_date', title: 'Pickup Date' },
        { id: 'rto_initiated_date', title: 'RTO-Initiate Date' },
        { id: 'aging', title: 'RTO Ageing' },
        { id: 'delivery_tat', title: 'TAT (In Days)' },
        { id: 'latest_status', title: 'Status' },
      ]
    });
    csvWriter
      .writeRecords(rtoInitiatedAgeing)
      .then(() => console.log('Data uploaded into csv successfully'));
    res.json({
      status: 200,
      data: 'http://' + hostname + '/RTO_Initiate_Ageing_Reports/' + fileName
    })
  } catch (error) {
    console.log(`Error: ${error.message}`)
    return res.status(400).json({
      message: 'Bad Request',
    })
  }
})

/*
    Get Delivery Ageing report using date range (import_date and booking_date)   (Date: 10-02-2022)
*/
router.get('/getDeliveryAgeingReport/:startDate/:endDate', async (req, res) => {
  var startDate = trim(req.params.startDate)
  var endDate = trim(req.params.endDate)
  const deliveryAgeing = [];
  try {
    const OrdersV1 = await ordersV1Model.find({
      "status.import_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      "status.dispatch_status": '5',
    });
    console.log("OrdersV1-----", OrdersV1.length)
    for await (let data of OrdersV1) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'Instashipin V2';
      var transaction_id = data.transaction_id ? data.transaction_id : null;
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        // console.log("consignorname---",consignorname)
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;

        if (status_delivery_date) {
          var delivery_date = moment(status_delivery_date).format('YYYY-MM-DD');
        } else {
          var delivery_date = '';
        }

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        if (status_import_date) {
          var import_date = moment(status_import_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }
        // if (status_rto_initiated_date != "" && status_rto_initiated_date >= '2019-12-01' && status_delivery_date != "" && status_delivery_date >= '2019-12-01') {
        const date1 = new Date();
        // console.log("date1----",date1)
        const date2 = new Date(status_delivery_date);
        const diffTime = Math.abs(date2 - date1);
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        // console.log(diffTime + " milliseconds");
        var deliverytat = diffDays;
        // console.log("deliverytat@@---",deliverytat);
        // } else {
        //     var deliverytat = '';
        // }

        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
        } else {
          var aging = '';
        }
      }
      if (data.package) {
        var package_actual_weight = data.package.actual_weight ? data.package.actual_weight : null;
        var package_item_description = data.package.item_description ? data.package.item_description : null;
        var package_items = data.package.items ? data.package.items : null;
        var package_pieces = data.package.pieces ? data.package.pieces : null;
        var package_product = data.package.product ? data.package.product : null;
        var product_value = data.package.product_value ? data.package.product_value : null;
        var package_rp = data.package.rp ? data.package.rp : null;
        var collectable_value = data.package.collectable_value ? data.package.collectable_value : null;

      }

      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var count = 0;
      count = tracking.length;
      if (count > 0) {
        count = count - 1;
        var status_remark = tracking[count]['remarks'];
      } else {
        var status_remark = 'PENDING PICKUP';
      }
      // console.log("status_remark---",status_remark)

      var order_type = "FORWARD";
      //  console.log("package_rp---",package_rp)
      if (package_rp && package_rp == 1) {
        var order_type = 'REVERSE';
        // console.log("order_type---",order_type)
      }
      await deliveryAgeing.push({
        airwaybilno: airwaybilno,
        sync_source: sync_source,
        import_date: import_date,
        pickup_date: pickup_date,
        delivery_date: delivery_date,
        transaction_id: transaction_id,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneelastname: consigneelastname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        packages_description: package_item_description,
        packages_quantity: package_pieces,
        product_value: product_value,
        payment_paytype: package_product,
        collectable_value: collectable_value,
        order_type: order_type,
        delivery_tat: deliverytat,
        aging: aging,
        latest_status: "DELIVERED",
      })


    }
    const OrdersV2 = await ordersV2Model.find({
      "status.booking_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      "status.dispatch_status": '5',
    })

    console.log("OrdersV2---", OrdersV2.length)
    for await (let data of OrdersV2) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var transaction_id = data.transaction_id ? data.transaction_id : null;
      var sync_source = data.sync_source ? data.sync_source : 'V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;

        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
        var collectable_value = data.payment.collectable ? data.payment.collectable : null;
        var product_value = data.payment.invoice_amount ? data.payment.invoice_amount : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_rp = data.status.rp ? data.status.rp : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;

        if (status_delivery_date) {
          var delivery_date = moment(status_delivery_date).format('YYYY-MM-DD');
        } else {
          var delivery_date = '';
        }

        if (status_booking_date) {
          var import_date = moment(status_booking_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        // if (status_rto_initiated_date != "" && status_rto_initiated_date >= '2019-12-01' && status_delivery_date != "" && status_delivery_date >= '2019-12-01') {
        const date1 = new Date();
        // console.log("date1----",date1)
        const date2 = new Date(status_delivery_date);
        const diffTime = Math.abs(date2 - date1);
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        // console.log(diffTime + " milliseconds");
        var deliverytat = diffDays;
        // console.log("deliverytat@@---",deliverytat);
        // } else {
        //     var deliverytat = '';
        // }
        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
        } else {
          var aging = '';
        }
      }

      var packages = data.packages ? data.packages : [];
      if (packages != '') {
        for (let pack of packages) {
          var packages_description = pack.description ? pack.description : null;
          var packages_quantity = pack.quantity ? pack.quantity : null;
          var packages_actual_weight = pack.actual_weight ? pack.actual_weight : null;
          var packages_price = pack.price ? pack.price : null;
          var total_quantity = 0, total_weight = 0, total_price = 0;
          // console.log("packages_quantity---",packages_quantity)
          total_price += (packages_price != '' ? parseFloat(packages_price) : 0);//product_value
          total_quantity += !isNumeric(packages_quantity) ? 1 : packages_quantity;
          total_weight += !isNumeric(packages_actual_weight) ? 0 : packages_actual_weight;
          // console.log("total_price---",total_price)
        }

      }

      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var count = 0;
      count = tracking.length;
      if (count > 0) {
        count = count - 1;
        var status_remark = tracking[count]['remarks'];
      } else {
        var status_remark = 'PENDING PICKUP';
      }
      // console.log("status_remark---",status_remark)
      var order_type = "FORWARD";

      await deliveryAgeing.push({
        airwaybilno: data.airwaybilno,
        sync_source: sync_source,
        transaction_id: transaction_id,
        import_date: import_date,
        pickup_date: pickup_date,
        delivery_date: delivery_date,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneelastname: consigneelastname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consigneetelephone2: consigneetelephone2,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        consignorsub_vendoraddress2: consignorsub_vendoraddress2,
        consignorsub_vendorcity: consignorsub_vendorcity,
        consignorsub_vendorname: consignorsub_vendorname,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorstate: consignorsub_vendorstate,
        courier_code: courier_code,
        orderno: orderno,
        order_type: order_type,
        packages_description: packages_description,
        // total_price: total_price,
        product_value: total_price,
        payment_paytype: payment_paytype,
        order_type: order_type,
        delivery_tat: deliverytat,
        aging: aging,
        latest_status: "DELIVERED",
      })
    }

    var date = moment(new Date()).format('yyyy-MM-DD');
    var hostname = req.headers.host;
    var currentPath = process.cwd();
    var dir = currentPath + '/src/public/Delivery_Ageing_Reports/';
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }
    var fileName = 'Delivery_Ageing_Report_' + startDate + '_' + endDate + '.csv';
    const csvWriter = await createCsvWriter({

      // Output csv file name is geek_data
      path: dir + fileName,
      options:
        { flags: 'a' },
      header: [

        // Title of the columns (column_names)
        { id: 'airwaybilno', title: 'AirwayBill No' },
        { id: 'airwaybilno', title: 'Invoice No' },
        { id: 'orderno', title: 'Order No' },
        { id: 'courier_code', title: 'Courier Name' },
        { id: 'consigneefirstname', title: 'Consignee First Name' },
        { id: 'consigneelastname', title: 'Consignee Last Name' },
        { id: 'consigneeaddress1', title: 'Consignee Address' },
        { id: 'consigneecity', title: 'Destination' },
        { id: 'consigneestate', title: 'State' },
        { id: 'consigneepincode', title: 'Destination Pincode' },
        { id: 'packages_description', title: 'Item Description' },
        { id: 'product_value', title: 'Product Amount' },
        { id: 'consignorsub_vendoraddress1', title: 'Pickup Location' },
        { id: 'import_date', title: 'Import Date' },
        { id: 'pickup_date', title: 'Pickup Date' },
        { id: 'delivery_date', title: 'Delivery Date' },
        { id: 'aging', title: 'Delivery Ageing' },
        { id: 'delivery_tat', title: 'TAT (In Days)' },
        { id: 'latest_status', title: 'Status' },
      ]
    });
    csvWriter
      .writeRecords(deliveryAgeing)
      .then(() => console.log('Data uploaded into csv successfully'));
    res.json({
      status: 200,
      data: 'http://' + hostname + '/Delivery_Ageing_Reports/' + fileName
    })
  } catch (error) {
    console.log(`Error: ${error.message}`)
    return res.status(400).json({
      message: 'Bad Request',
    })
  }
})

/*
    Get Failed NDR report using date range (pickup_date)   (Date: 10-02-2022)
*/
router.get('/getFailedNDRReport/:startDate/:endDate', async (req, res) => {
  var startDate = trim(req.params.startDate)
  var endDate = trim(req.params.endDate)
  const failedNdr = [];
  try {
    const OrdersV1 = await ordersV1Model.find({
      "status.pickup_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
    });
    console.log("OrdersV1-----", OrdersV1.length)
    for await (let data of OrdersV1) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'Instashipin V2';
      var transaction_id = data.transaction_id ? data.transaction_id : null;
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        // console.log("consignorname---",consignorname)
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }
      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;

        if (status_delivery_date) {
          var delivery_date = moment(status_delivery_date).format('YYYY-MM-DD');
        } else {
          var delivery_date = '';
        }

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        if (status_import_date) {
          var import_date = moment(status_import_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }
        // if (status_rto_initiated_date != "" && status_rto_initiated_date >= '2019-12-01' && status_delivery_date != "" && status_delivery_date >= '2019-12-01') {
        const date1 = new Date();
        // console.log("date1----",date1)
        const date2 = new Date(status_delivery_date);
        const diffTime = Math.abs(date2 - date1);
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        // console.log(diffTime + " milliseconds");
        var deliverytat = diffDays;
        // console.log("deliverytat@@---",deliverytat);
        // } else {
        //     var deliverytat = '';
        // }

        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
        } else {
          var aging = '';
        }
      }
      if (data.package) {
        var package_actual_weight = data.package.actual_weight ? data.package.actual_weight : null;
        var package_item_description = data.package.item_description ? data.package.item_description : null;
        var package_items = data.package.items ? data.package.items : null;
        var package_pieces = data.package.pieces ? data.package.pieces : null;
        var package_product = data.package.product ? data.package.product : null;
        var product_value = data.package.product_value ? data.package.product_value : null;
        var package_rp = data.package.rp ? data.package.rp : null;
        var collectable_value = data.package.collectable_value ? data.package.collectable_value : null;

      }

      var ndr_attempt = data.ndr_attempt ? data.ndr_attempt : [];
      // console.log("ndr_attempt---",ndr_attempt)
      if (ndr_attempt != '') {
        for (let ndr of ndr_attempt) {
          var ndr_rdate = ndr.rdate ? ndr.rdate : null;
          var ndr_status = ndr.status ? ndr.status : null;
          var ndr_message = ndr.message ? ndr.message : null;
          var ndr_result = ndr.result ? ndr.result : null;
          if (ndr_result) {
            var ndr_result_status_code = ndr.result.status_code ? ndr.result.status_code : null;
            var ndr_result_tracking_no = ndr.result.tracking_no ? ndr.result.tracking_no : null;
            var ndr_result_error = ndr.result.error ? ndr.result.error : null;
            var ndr_result_status = ndr.result.status ? ndr.result.status : null;
            var ndr_result_massage = ndr.result.massage ? ndr.result.message : null;
            var ndr_result_remarks = ndr.result.remarks ? ndr.result.remarks : null;
          }
          if (ndr_message == "failure") {
            var latest_status = 'UNDELIVERED';
          } else {
            var latest_status = '';
          }

          if (ndr_result_massage && ndr_result_massage != null) {
            var fail_reason = ndr_result_massage;
          }
          else {
            var fail_reason = ndr_result_remarks[0];
          }
        }
      }

      await failedNdr.push({
        airwaybilno: airwaybilno,
        sync_source: sync_source,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consignorname: consignorname,
        consignorsub_vendorname: consignorsub_vendorname,
        courier_code: courier_code,
        orderno: orderno,
        packages_description: package_item_description,
        latest_status: latest_status,
        fail_reason: fail_reason
      })


    }
    const OrdersV2 = await ordersV2Model.find({
      "status.pickup_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      }
    })

    console.log("OrdersV2---", OrdersV2.length)
    for await (let data of OrdersV2) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var transaction_id = data.transaction_id ? data.transaction_id : null;
      var sync_source = data.sync_source ? data.sync_source : 'V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;

        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }
      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
        var collectable_value = data.payment.collectable ? data.payment.collectable : null;
        var product_value = data.payment.invoice_amount ? data.payment.invoice_amount : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_rp = data.status.rp ? data.status.rp : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;

        if (status_delivery_date) {
          var delivery_date = moment(status_delivery_date).format('YYYY-MM-DD');
        } else {
          var delivery_date = '';
        }

        if (status_booking_date) {
          var import_date = moment(status_booking_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        // if (status_rto_initiated_date != "" && status_rto_initiated_date >= '2019-12-01' && status_delivery_date != "" && status_delivery_date >= '2019-12-01') {
        const date1 = new Date();
        // console.log("date1----",date1)
        const date2 = new Date(status_delivery_date);
        const diffTime = Math.abs(date2 - date1);
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        // console.log(diffTime + " milliseconds");
        var deliverytat = diffDays;
        // console.log("deliverytat@@---",deliverytat);
        // } else {
        //     var deliverytat = '';
        // }
        if (deliverytat <= "6") {
          var aging = 'Day ' + deliverytat
        } else if (deliverytat > 6 && deliverytat < 16) {
          var aging = '7 To 15 Days';
        } else if (deliverytat > 15 && deliverytat < 31) {
          var aging = '16 To 30 Days';
        } else if (deliverytat > 30) {
          var aging = '>30 Days';
        } else {
          var aging = '';
        }
      }

      var packages = data.packages ? data.packages : [];
      if (packages != '') {
        for (let pack of packages) {
          var packages_description = pack.description ? pack.description : null;
          var packages_quantity = pack.quantity ? pack.quantity : null;
          var packages_actual_weight = pack.actual_weight ? pack.actual_weight : null;
          var packages_price = pack.price ? pack.price : null;
          var total_quantity = 0, total_weight = 0, total_price = 0;
          // console.log("packages_quantity---",packages_quantity)
          total_price += (packages_price != '' ? parseFloat(packages_price) : 0);//product_value
          total_quantity += !isNumeric(packages_quantity) ? 1 : packages_quantity;
          total_weight += !isNumeric(packages_actual_weight) ? 0 : packages_actual_weight;
          // console.log("total_price---",total_price)
        }

      }

      var ndr_attempt = data.ndr_attempt ? data.ndr_attempt : [];
      // console.log("ndr_attempt---",ndr_attempt)
      if (ndr_attempt != '') {
        for (let ndr of ndr_attempt) {
          var ndr_rdate = ndr.rdate ? ndr.rdate : null;
          var ndr_status = ndr.status ? ndr.status : null;
          var ndr_message = ndr.message ? ndr.message : null;
          var ndr_result = ndr.result ? ndr.result : null;
          if (ndr_result) {
            var ndr_result_status_code = ndr.result.status_code ? ndr.result.status_code : null;
            var ndr_result_tracking_no = ndr.result.tracking_no ? ndr.result.tracking_no : null;
            var ndr_result_error = ndr.result.error ? ndr.result.error : null;
            var ndr_result_status = ndr.result.status ? ndr.result.status : null;
            var ndr_result_massage = ndr.result.massage ? ndr.result.message : null;
            var ndr_result_remarks = ndr.result.remarks ? ndr.result.remarks : null;
          }
          if (ndr_message == "failure") {
            var latest_status = 'UNDELIVERED';
          } else {
            var latest_status = '';
          }

          if (ndr_result_massage && ndr_result_massage != null) {
            var fail_reason = ndr_result_massage;
          }
          else {
            var fail_reason = ndr_result_remarks[0];
          }
        }
      }

      await failedNdr.push({
        airwaybilno: data.airwaybilno,
        sync_source: sync_source,
        consigneeaddress1: consigneeaddress1,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consignorname: consignorname,
        consignorsub_vendorname: consignorsub_vendorname,
        courier_code: courier_code,
        orderno: orderno,
        packages_description: packages_description,
        latest_status: latest_status,
        fail_reason: fail_reason,
      })
    }

    var date = moment(new Date()).format('yyyy-MM-DD');
    var hostname = req.headers.host;
    var currentPath = process.cwd();
    var dir = currentPath + '/src/public/Failed_NDR_reports/';
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }
    var fileName = 'Failed_NDR_report_' + startDate + '_' + endDate + '.csv';
    const csvWriter = await createCsvWriter({

      // Output csv file name is geek_data
      path: dir + fileName,
      options:
        { flags: 'a' },
      header: [

        // Title of the columns (column_names)
        { id: 'sync_source', title: 'Data Source' },
        { id: 'courier_code', title: 'LSP Code' },
        { id: 'airwaybilno', title: 'AWB' },
        { id: 'orderno', title: 'Order No' },
        { id: 'courier_code', title: 'Courier Name' },
        { id: 'consignorsub_vendorname', title: 'Sub Vendor Name' },
        { id: 'consigneefirstname', title: 'Consignee First Name' },
        { id: 'consigneeaddress1', title: 'Consignee Address' },
        { id: 'consigneecity', title: 'Destination City' },
        { id: 'consigneestate', title: 'State' },
        { id: 'consigneepincode', title: 'Destination Pincode' },
        { id: 'consigneetelephone1', title: 'Consignee Phone1' },
        { id: 'packages_description', title: 'Product Description' },
        { id: 'latest_status', title: 'Latest Status' },
        { id: 'failure_reason', title: 'Failure Reason' },
      ]
    });
    csvWriter
      .writeRecords(failedNdr)
      .then(() => console.log('Data uploaded into csv successfully'));
    res.json({
      status: 200,
      data: 'http://' + hostname + '/Failed_NDR_reports/' + fileName
    })
  } catch (error) {
    console.log(`Error: ${error.message}`)
    return res.status(400).json({
      message: 'Bad Request',
    })
  }
})

/*
    Get Reattempt report using date range (import_date and booking_date)   (Date: 10-02-2022)
*/
router.get('/getReattemptReport/:startDate/:endDate', async (req, res) => {
  var startDate = trim(req.params.startDate)
  var endDate = trim(req.params.endDate)
  const reattempt = [];
  try {
    const OrdersV1 = await ordersV1Model.find({
      "status.import_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      "status.dispatch_status": '18'

    });
    console.log("OrdersV1-----", OrdersV1.length)
    for await (let data of OrdersV1) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'Instashipin V2';
      var transaction_id = data.transaction_id ? data.transaction_id : null;
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        // console.log("consignorname---",consignorname)
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_last_attempt_date = data.status.last_attempt_date ? data.status.last_attempt_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;


        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        if (status_import_date) {
          var import_date = moment(status_import_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }
        // if (status_pickup_date != "" && status_pickup_date >= '2019-12-01' && status_delivery_date != "" && status_delivery_date >= '2019-12-01') {
        //   const date1 = new Date();
        //   // console.log("date1----",date1)
        //   const date2 = new Date(status_pickup_date);
        //   const diffTime = Math.abs(date2 - date1);
        //   const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        //   // console.log(diffTime + " milliseconds");
        //   var deliverytat = diffDays;
        //   // console.log("deliverytat@@---",deliverytat);
        // } else {
        //   var deliverytat = '';
        // }

        // if (deliverytat <= "6") {
        //   var aging = 'Day ' + deliverytat
        // } else if (deliverytat > 6 && deliverytat < 16) {
        //   var aging = '7 To 15 Days';
        // } else if (deliverytat > 15 && deliverytat < 31) {
        //   var aging = '16 To 30 Days';
        // } else if (deliverytat > 30) {
        //   var aging = '>30 Days';
        // } else {
        //   var aging = '';
        // }

      }
      if (data.package) {
        var package_actual_weight = data.package.actual_weight ? data.package.actual_weight : null;
        var package_item_description = data.package.item_description ? data.package.item_description : null;
        var package_items = data.package.items ? data.package.items : null;
        var package_pieces = data.package.pieces ? data.package.pieces : null;
        var package_product = data.package.product ? data.package.product : null;
        var product_value = data.package.product_value ? data.package.product_value : null;
        var package_rp = data.package.rp ? data.package.rp : null;
        var collectable_value = data.package.collectable_value ? data.package.collectable_value : null;

      }

      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var count = 0;
      count = tracking.length;
      if (count > 0) {
        count = count - 1;
        var status_remark = tracking[count]['remarks'];
      } else {
        var status_remark = 'PENDING PICKUP';
      }
      // console.log("status_remark---",status_remark)

      var order_type = "FORWARD";
      //  console.log("package_rp---",package_rp)
      if (package_rp && package_rp == 1) {
        var order_type = 'REVERSE';
        // console.log("order_type---",order_type)
      }

      await reattempt.push({
        airwaybilno: airwaybilno,
        sync_source: sync_source,
        import_date: import_date,
        pickup_date: pickup_date,
        transaction_id: transaction_id,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneelastname: consigneelastname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        courier_code: courier_code,
        orderno: orderno,
        packages_description: package_item_description,
        packages_quantity: package_pieces,
        product_value: product_value,
        payment_paytype: package_product,
        collectable_value: collectable_value,
        order_type: order_type,
        status_remark: status_remark,
        latest_status: "REATTEMPT",
      })


    }
    const OrdersV2 = await ordersV2Model.find({
      "status.booking_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      "status.dispatch_status": '18'
    })

    console.log("OrdersV2---", OrdersV2.length)
    for await (let data of OrdersV2) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var orderno = data.orderno ? data.orderno : null;
      var transaction_id = data.transaction_id ? data.transaction_id : null;
      var sync_source = data.sync_source ? data.sync_source : 'V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
        var consigneetelephone2 = data.consignee.telephone2 ? data.consignee.telephone2 : null;
        // console.log("consigneeaddress1------",consigneeaddress1)
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;

        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendoraddress1 = data.consignor.sub_vendor.address1 ? data.consignor.sub_vendor.address1 : null;
          var consignorsub_vendoraddress2 = data.consignor.sub_vendor.address2 ? data.consignor.sub_vendor.address2 : null;
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;
          var consignorsub_vendorstate = data.consignor.sub_vendor.state ? data.consignor.sub_vendor.state : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }

      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
        var collectable_value = data.payment.collectable ? data.payment.collectable : null;
        var product_value = data.payment.invoice_amount ? data.payment.invoice_amount : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_rp = data.status.rp ? data.status.rp : null;
        var status_rto_intransit_date = data.status.rto_intransit_date ? data.status.rto_intransit_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_last_attempt_date = data.status.last_attempt_date ? data.status.last_attempt_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;


        if (status_booking_date) {
          var import_date = moment(status_booking_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }

        if (status_pickup_date) {
          var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
        } else {
          var pickup_date = '';
        }

        // if (status_pickup_date != "" && status_pickup_date >= '2019-12-01' && status_delivery_date != "" && status_delivery_date >= '2019-12-01') {
        //   const date1 = new Date();
        //   // console.log("date1----",date1)
        //   const date2 = new Date(status_pickup_date);
        //   const diffTime = Math.abs(date2 - date1);
        //   const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        //   // console.log(diffTime + " milliseconds");
        //   var deliverytat = diffDays;
        //   // console.log("deliverytat@@---",deliverytat);
        // } else {
        //   var deliverytat = '';
        // }

        // if (deliverytat == "0") {
        //   var aging = 'Day ' + deliverytat
        // } else if (deliverytat == "1") {
        //   var aging = 'Day ' + deliverytat
        // } else if (deliverytat == "2") {
        //   var aging = 'Day ' + deliverytat
        // } else if (deliverytat == "3") {
        //   var aging = 'Day ' + deliverytat
        // } else if (deliverytat == "4") {
        //   var aging = 'Day ' + deliverytat
        // } else if (deliverytat == "5") {
        //   var aging = 'Day ' + deliverytat
        // } else if (deliverytat == "6") {
        //   var aging = 'Day ' + deliverytat
        // } else if (deliverytat > 6 && deliverytat < 16) {
        //   var aging = '7 To 15 Days';
        // } else if (deliverytat > 15 && deliverytat < 31) {
        //   var aging = '16 To 30 Days';
        // } else if (deliverytat > 30) {
        //   var aging = '>30 Days';
        // } else {
        //   var aging = '';
        // }
      }

      var packages = data.packages ? data.packages : [];
      if (packages != '') {
        for (let pack of packages) {
          var packages_description = pack.description ? pack.description : null;
          var packages_quantity = pack.quantity ? pack.quantity : null;
          var packages_actual_weight = pack.actual_weight ? pack.actual_weight : null;
          var packages_price = pack.price ? pack.price : null;
          var total_quantity = 0, total_weight = 0, total_price = 0;
          // console.log("packages_quantity---",packages_quantity)
          total_price += (packages_price != '' ? parseFloat(packages_price) : 0);//product_value
          total_quantity += !isNumeric(packages_quantity) ? 1 : packages_quantity;
          total_weight += !isNumeric(packages_actual_weight) ? 0 : packages_actual_weight;
          // console.log("total_price---",total_price)
        }

      }

      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var count = 0;
      count = tracking.length;
      if (count > 0) {
        count = count - 1;
        var status_remark = tracking[count]['remarks'];
      } else {
        var status_remark = 'PENDING PICKUP';
      }
      // console.log("status_remark---",status_remark)
      var order_type = "FORWARD";

      await reattempt.push({
        airwaybilno: data.airwaybilno,
        sync_source: sync_source,
        transaction_id: transaction_id,
        import_date: import_date,
        pickup_date: pickup_date,
        consigneeaddress1: consigneeaddress1,
        consigneecity: consigneecity,
        consigneefirstname: consigneefirstname,
        consigneelastname: consigneelastname,
        consigneepincode: consigneepincode,
        consigneestate: consigneestate,
        consigneetelephone1: consigneetelephone1,
        consignorname: consignorname,
        consignorsub_vendoraddress1: consignorsub_vendoraddress1,
        courier_code: courier_code,
        orderno: orderno,
        order_type: order_type,
        packages_description: packages_description,
        // total_price: total_price,
        product_value: total_price,
        payment_paytype: payment_paytype,
        order_type: order_type,
        // delivery_tat: deliverytat,
        // aging: aging,
        status_remark: status_remark,
        latest_status: "REATTEMPT",
      })
    }

    var date = moment(new Date()).format('yyyy-MM-DD');
    var hostname = req.headers.host;
    var currentPath = process.cwd();
    var dir = currentPath + '/src/public/Reattempt_Reports/';
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }
    var fileName = 'Reattempt_Report_' + startDate + '_' + endDate + '.csv';
    const csvWriter = await createCsvWriter({

      // Output csv file name is geek_data
      path: dir + fileName,
      options:
        { flags: 'a' },
      header: [

        // Title of the columns (column_names)
        { id: 'transaction_id', title: 'Transaction No' },
        { id: 'order_type', title: 'Order Type' },
        { id: 'airwaybilno', title: 'AirwayBill No' },
        { id: 'orderno', title: 'Order No' },
        { id: 'courier_code', title: 'Courier Name' },
        { id: 'consignorname', title: 'Client Name' },
        { id: 'consigneefirstname', title: 'Consignee First Name' },
        { id: 'consigneelastname', title: 'Consignee Last Name' },
        { id: 'consigneeaddress1', title: 'Consignee Address' },
        { id: 'consigneecity', title: 'Destination' },
        { id: 'consigneestate', title: 'State' },
        { id: 'consigneepincode', title: 'Destination Pincode' },
        { id: 'consigneetelephone1', title: 'Consignee Phone 1' },
        { id: 'packages_description', title: 'Item Description' },
        { id: 'product_value', title: 'Product Amount' },
        { id: 'consignorsub_vendoraddress1', title: 'Pickup Location' },
        { id: 'import_date', title: 'Import Date' },
        { id: 'pickup_date', title: 'Pickup Date' },
        { id: 'latest_status', title: 'Status' },
      ]
    });
    csvWriter
      .writeRecords(reattempt)
      .then(() => console.log('Data uploaded into csv successfully'));
    res.json({
      status: 200,
      data: 'http://' + hostname + '/Reattempt_Reports/' + fileName
    })
  } catch (error) {
    console.log(`Error: ${error.message}`)
    return res.status(400).json({
      message: 'Bad Request',
    })
  }
})

/*
    Get Order Generated report using date range (import_date and booking_date)   (Date: 11-02-2022)
*/
router.get('/getOrderGenerated/:startDate/:endDate', async (req, res) => {
  var startDate = trim(req.params.startDate)
  var endDate = trim(req.params.endDate)
  const orderGenerate = [];
  try {
    const OrdersV1 = await ordersV1Model.find({
      "status.import_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      $or: [
        {
          "status.dispatch_status": {
            "$gte": "0",
          }
        },
        {
          "status.dispatch_status": '0'
        },
        {
          'airwaybilno': {
            "$ne": 'null'
          }
        },
      ],

    });
    console.log("OrdersV1---", OrdersV1.length)
    for await (let data of OrdersV1) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var api_serviceable_list = api_serviceable_list ? api_serviceable_list : null
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'Instashipin V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
        var consigneelastname = data.consignee.lastname ? data.consignee.lastname : null;
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        var consignorrto = data.consignor.rto ? data.consignor.rto : null;
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }
      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;
        var status_last_attempt_date = data.status.last_attempt_date ? data.status.last_attempt_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_rto_delivery_date = data.status.rto_delivery_date ? data.status.rto_delivery_date : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_rto_intransit_date = data.status.rto_intransit_date ? data.status.rto_intransit_date : null;
        var status_rto_tracking_no = data.status.rto_tracking_no ? data.status.rto_tracking_no : null;
        var status_zone_code = data.status.zone_code ? data.status.zone_code : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        if (status_delivery_date) {
          var delivery_date = moment(status_delivery_date).format('YYYY-MM-DD');
        } else {
          var delivery_date = ''
        }
        if (status_last_attempt_date) {
          var last_attempt_date = moment(status_last_attempt_date).format('YYYY-MM-DD');
        } else {
          var last_attempt_date = '';
        }
        if (status_import_date) {
          var import_date = moment(status_import_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }
        if (status_rto_initiated_date) {
          var initiated_date = moment(status_rto_initiated_date).format('YYYY-MM-DD');
        } else {
          var initiated_date = '';
        }
        if (status_rto_intransit_date) {
          var intransit_date = moment(status_rto_intransit_date).format('YYYY-MM-DD');
        } else {
          var intransit_date = '';
        }
        if (status_rto_delivery_date) {
          var rto_delivery_date = moment(status_rto_delivery_date).format('YYYY-MM-DD');
        } else {
          var rto_delivery_date = '';
        }

        if (status_delivery_date && status_delivery_date != "0000-00-00 00:00:00" && status_delivery_date != "" && status_pickup_date && status_pickup_date != "0000-00-00 00:00:00" && status_pickup_date != "") {
          const date1 = new Date(status_pickup_date);
          const date2 = new Date(status_delivery_date);
          const diffTime = Math.abs(date2 - date1);
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          // console.log(diffTime + " milliseconds");
          var deliverytat = diffDays
          // console.log("deliverytat@@---",deliverytat + " days");
        } else {
          var deliverytat = "";
          // console.log("deliverytat----",deliverytat)
        }
        if (deliverytat <= "6") {
          var tat = 'Day ' + deliverytat
          //  console.log("tat1---",tat)
        } else if (deliverytat > 6 && deliverytat < 16) {
          var tat = '7 To 15 Days';
          //  console.log("tat2---",tat)
        } else if (deliverytat > 15 && deliverytat < 31) {
          var tat = '16 To 30 Days';
          //  console.log("tat3---",tat)
        } else if (deliverytat > 30) {
          var tat = '>30 Days';
          //  console.log("tat4---",tat)
        } else {
          var tat = '';
          // console.log("tat5---",tat)
        }
      }
      if (data.weight_update) {
        var weightupdate_actual_weight = data.weight_update.actual_weight ? data.weight_update.actual_weight : null;
      }
      if (data.package) {
        var package_actual_weight = data.package.actual_weight ? data.package.actual_weight : null;
        var package_item_description = data.package.item_description ? data.package.item_description : null;
        var package_items = data.package.items ? data.package.items : null;
        var package_pieces = data.package.pieces ? data.package.pieces : null;
        var package_product = data.package.product ? data.package.product : null;
        var package_rp = data.package.rp ? data.package.rp : null;
        var package_volumetric_weight = data.package.volumetric_weight ? data.package.volumetric_weight : null;

      }
      if (data.payments) {
        var payments_cod_collect = data.payments.cod_collect ? data.payments.cod_collect : null;
        if (payments_cod_collect) {
          var payments_codcollect_cod_recived = data.payments.cod_collect.cod_recived ? data.payments.cod_collect.cod_recived : null;
        }
        var payments_cod_payment = data.payments.cod_payment ? data.payments.cod_payment : null;
        if (payments_cod_payment) {
          var payments_codpayment_cod_paid = data.payments.cod_payment.cod_paid ? data.payments.cod_payment.cod_paid : null;
          var payments_codpayment_paidbankrefno = data.payments.cod_payment.paid_bank_ref_no ? data.payments.cod_payment.paid_bank_ref_no : null;
          var payments_codpayment_paiddatetime = data.payments.cod_payment.paid_datetime ? data.payments.cod_payment.paid_datetime : null;
          if (payments_codpayment_paiddatetime) {
            var payments_codpayment_paiddate = moment(payments_codpayment_paiddatetime).format('YYYY-MM-DD');
          } else {
            var payments_codpayment_paiddate = '';
          }
        }

      }
      var order_type = "FORWARD";
      //  console.log("package_rp---",package_rp)
      if (package_rp && package_rp == 1) {
        var order_type = 'REVERSE';
        // console.log("order_type---",order_type)
      }
      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)
      var first_ofd_date = sec_ofd_date = third_ofd_date = last_ofd_date = '';
      var i = 0;
      var count = 0;
      if (tracking != '') {
        // console.log("tracking---",tracking)
        var statuscode = '305';
        var no_of_attempt = 0;
        var tracking_counts = [];
        var obj = {}
        tracking.forEach(function (item) {
          obj[item.status_code] ? obj[item.status_code]++ : obj[item.status_code] = 1;
        });
        // console.log("obj----",obj)  
        // console.log("count---",obj[statuscode])
        no_of_attempt = obj[statuscode];
        // console.log("tracking[0]----",tracking[0])
        for await (let track of tracking) {
          // var track_lsp_status_code = track.lsp_status_code ? track.lsp_status_code : null;
          var track_status = track.status ? track.status : null;
          var track_status_code = track.status_code ? track.status_code : null;
          // var track_lsp_status = track.lsp_status ? track.lsp_status : null;
          var track_parent_status_code = track.parent_status_code ? track.parent_status_code : null;
          // var track_source_status_code = track.source_status_code ? track.source_status_code : null;
          var track_remarks = track.remarks ? track.remarks : null;
          var track_updated_date = track.updated_date ? track.updated_date : null;
          var track_location = track.location ? track.location : null;
          // var track_manualentry = track.manualentry ? track.manualentry : null;
          // console.log("track_status_code.length----",track_status_code.length)

          // console.log("track_status_code----",track_status_code)
          if (track_status_code == '305') {
            if (i == 0) {
              var first_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("first_ofd_date----",first_ofd_date)
            } else if (i == 1) {
              var sec_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("sec_ofd_date----",sec_ofd_date)
            } else if (i == 2) {
              var third_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("third_ofd_date----",third_ofd_date)
            }
            var last_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
            i++;
            // console.log("i---",i)
          } else {
            var first_ofd_date = first_ofd_date;
            var sec_ofd_date = sec_ofd_date;
            var third_ofd_date = third_ofd_date;
            var last_ofd_date = last_ofd_date;
          }
        }
        count = tracking.length;
        if (count > 0) {
          count = count - 1;
          var status_remark = tracking[count]['remarks'];
        } else {
          var status_remark = 'PENDING PICKUP';
        }

        if (track_status_code == "I002" || track_status_code == "I003" || track_status_code == "I006" || track_status_code == "I007") {
          // latest_status = 'UNDELIVERED';
          ndr_action = tracking[count]['remarks'];
          ndr_action_date = moment(tracking[count]['track_updated_date']).format('YYYY-MM-DD');
          // console.log("ndr_action---",ndr_action)
          // console.log("ndr_action_date---",ndr_action_date)
        } else if (status_dispatch_status == "18") {
          ndr_action = 'CL-REATTEMPT';
          ndr_action_date = '';
        } else {
          ndr_action = '';
          ndr_action_date = '';
        }

      }
      // console.log("status_dispatch_status---",status_dispatch_status)
      var latest_status = '';
      if (status_dispatch_status == '0') {
        latest_status = "PENDING PICKUP";
      } else if (status_dispatch_status == '1') {
        latest_status = "PICKUP DONE";
      } else if (status_dispatch_status == '2') {
        latest_status = "IN-TRANSIT";
      } else if (status_dispatch_status == '4') {
        latest_status = "OUT FOR DELIVERY"
      } else if (status_dispatch_status == '5') {
        latest_status = "DELIVERED"
        // console.log("latest_status---",latest_status)
      } else if (status_dispatch_status == '6') {
        latest_status = "UNDELIVERED"
      } else if (status_dispatch_status == '7') {
        latest_status = "UNDELIVERED"
      } else if (status_dispatch_status == '10') {
        latest_status = "RTO INITIATED"
      } else if (status_dispatch_status == '11') {
        latest_status = "RTO IN-TRANSIT"
      } else if (status_dispatch_status == '12') {
        latest_status = "RTO OUT FOR DELIVERY"
      } else if (status_dispatch_status == '13') {
        latest_status = "RTO DELIVERED"
      } else if (status_dispatch_status == '14') {
        latest_status = "PICKUP CANCEL BY CLIENT"
      } else if (status_dispatch_status == '15') {
        latest_status = "LOST"
      } else if (status_dispatch_status == '16') {
        latest_status = "SHIPMENT DAMAGE"
      } else if (status_dispatch_status == '17') {
        latest_status = "DANGER GOODS"
      } else if (status_dispatch_status == '18') {
        latest_status = "REATTEMPT"
      } else if (status_dispatch_status == '19') {
        latest_status = "RTO"
      } else if (status_dispatch_status == '20') {
        latest_status = "RTO"
      } else if (status_dispatch_status == '21') {
        latest_status = "SELF COLLECT (CS)"
      } else if (status_dispatch_status == '55') {
        latest_status = "RTO UNDELIVERED"
      } else if (status_dispatch_status == '56') {
        latest_status = "REVERSE UNDELIVERED"
      } else if (status_dispatch_status == '1001') {
        latest_status = "UNATTEMPTED"
      } else if (status_dispatch_status == '8') {
        latest_status = "UNDELIVERED"
      } else {
        latest_status = "PENDING PICKUP";
        // status_remark="PENDING PICKUP";
      }



      if (status_pickup_date && latest_status && track_parent_status_code && track_parent_status_code != '14') {
        var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
      } else {
        var pickup_date = '';
      }

      // console.log("latest_status----",latest_status)
      var latest_status_date = track_updated_date;
      var latest_status_code = '99';
      if (track_status_code) {
        var latest_status_code = track_status_code;
      } else {
        var latest_status_code = '';
      }
      var latest_parent_status_code = '0';
      if (track_parent_status_code) {
        latest_parent_status_code = track_parent_status_code;
      } else {
        latest_parent_status_code = '';
      }

      // console.log("track_status_code---",track_status_code)

      var status_remark, last_undel_reason, ndr_action, ndr_action_date = ''
      // if ((track_parent_status_code == "10" || track_parent_status_code == "18")) {
      //   status_remark = track_remarks;
      //   // console.log("status_remark---",status_remark)
      // } else {
      //   status_remark = '';
      // }
      // console.log("track_parent_status_code------",track_parent_status_code)
      if (track_parent_status_code != '' && (track_parent_status_code == "6" || track_parent_status_code == "55")) {
        last_undel_reason = track_remarks;
        // console.log("last_undel_reason---",last_undel_reason)
      } else {
        last_undel_reason = '';
      }

      if (latest_status_code && (latest_status_code == "I002" || latest_status_code == "I003" || latest_status_code == "I004" || latest_status_code == "I005" || latest_status_code == "I006" || latest_status_code == "I007")) {
        latest_status = 'UNDELIVERED';
      }
      if (track_status_code && (track_status_code == '901' || track_status_code == '902') && track_updated_date && track_updated_date != '') {
        var lost_damage_date = moment(track_updated_date).format('YYYY-MM-DD');
        // console.log("lost_damage_date---",lost_damage_date)
      } else {
        var lost_damage_date = "";
        // console.log("lost_damage_date@@---",lost_damage_date)
      }

      const masters = [
        { dispatch_status_code: 99, status: "PENDING PICKUP", remarks: "pending pickup", dispatch_status: 0 },
        { dispatch_status_code: 100, status: "PICKUP DONE", remarks: "pickup done", dispatch_status: 1 },
        { dispatch_status_code: 102, status: "IN-TRANSIT", remarks: "processing at origin hub", dispatch_status: 2 },
        { dispatch_status_code: 305, status: "OUT FOR DELIVERY", remarks: "out for delivery", dispatch_status: 4 },
        { dispatch_status_code: 400, status: "Delivered", remarks: "delivered", dispatch_status: 5 },
        { dispatch_status_code: 401, status: "RTO Delivered", remarks: "rto delivered", dispatch_status: 13 },
        { dispatch_status_code: 500, status: "UNDELIVERED", remarks: "consignee refused", dispatch_status: 6 },
        { dispatch_status_code: 501, status: "UNDELIVERED", remarks: "incomplete address", dispatch_status: 6 },
        { dispatch_status_code: 503, status: "UNDELIVERED", remarks: "oda", dispatch_status: 6 },
        { dispatch_status_code: 504, status: "UNDELIVERED", remarks: "consignee shifted", dispatch_status: 6 },
        { dispatch_status_code: 505, status: "UNDELIVERED", remarks: "DAMAGED", dispatch_status: 6 },
        { dispatch_status_code: 506, status: "UNDELIVERED", remarks: "no such consignee", dispatch_status: 6 },
        { dispatch_status_code: 507, status: "UNDELIVERED", remarks: "future delivery", dispatch_status: 6 },
        { dispatch_status_code: 508, status: "UNDELIVERED", remarks: "cod not ready", dispatch_status: 6 },
        { dispatch_status_code: 509, status: "UNDELIVERED", remarks: "residence/office closed", dispatch_status: 6 },
        { dispatch_status_code: 510, status: "UNDELIVERED", remarks: "out of station", dispatch_status: 6 },
        { dispatch_status_code: 511, status: "UNDELIVERED", remarks: "shipment lost", dispatch_status: 6 },
        { dispatch_status_code: 512, status: "UNDELIVERED", remarks: "Dangerous Goods", dispatch_status: 6 },
        { dispatch_status_code: 513, status: "UNDELIVERED", remarks: "Self Collect", dispatch_status: 6 },
        { dispatch_status_code: 514, status: "UNDELIVERED", remarks: "Held With Govt Authority", dispatch_status: 6 },
        { dispatch_status_code: 515, status: "UNDELIVERED", remarks: "consignee not available", dispatch_status: 6 },
        { dispatch_status_code: 516, status: "UNDELIVERED", remarks: "consignee not responding", dispatch_status: 6 },
        { dispatch_status_code: 517, status: "UNDELIVERED", remarks: "misroute", dispatch_status: 6 },
        { dispatch_status_code: 518, status: "UNDELIVERED", remarks: "on hold", dispatch_status: 6 },
        { dispatch_status_code: 519, status: "UNDELIVERED", remarks: "restricted area", dispatch_status: 6 },
        { dispatch_status_code: 520, status: "UNDELIVERED", remarks: "snatched by consignee", dispatch_status: 6 },
        { dispatch_status_code: 521, status: "UNDELIVERED", remarks: "disturbance/natural disaster/strike/COVID", dispatch_status: 6 },
        { dispatch_status_code: 522, status: "UNDELIVERED", remarks: "Open Delivery", dispatch_status: 6 },
        { dispatch_status_code: 523, status: "UNDELIVERED", remarks: "Customer denied - OTP Delivery", dispatch_status: 6 },
        { dispatch_status_code: 524, status: "UNDELIVERED", remarks: "Time Constraint / Dispute", dispatch_status: 6 },
        { dispatch_status_code: 600, status: "RTO INITIATED", remarks: "rto initiated ", dispatch_status: 10 },
        { dispatch_status_code: 601, status: "RTO IN-TRANSIT", remarks: "rto intransit ", dispatch_status: 11 },
        { dispatch_status_code: 615, status: "RTO UNDELIVERED ", remarks: "Vendor refused ", dispatch_status: 55 },
        { dispatch_status_code: 900, status: "Pickup Cancelled ", remarks: "Pickup Cancelled by Client ", dispatch_status: 14 },
        { dispatch_status_code: 901, status: "LOST ", remarks: "SHIPMENT LOST ", dispatch_status: 15 },
        { dispatch_status_code: 902, status: "SHIPMENT DAMAGE ", remarks: "SHIPMENT DAMAGE ", dispatch_status: 16 },
        { dispatch_status_code: 951, status: "REATTEMPT ", remarks: "Reattempt ", dispatch_status: 18 },
        { dispatch_status_code: 1001, status: "Unattempted ", remarks: "Unattempted ", dispatch_status: 1001 },
        { dispatch_status_code: "I001", status: "SWC ", remarks: "Shared with Client ", dispatch_status: 7 },
        { dispatch_status_code: "I002", status: "CL-REATTEMPT ", remarks: "Client Reattempt ", dispatch_status: 7 },
        { dispatch_status_code: "I003", status: "CL-RTO-INITIATED ", remarks: "cl rto initiated ", dispatch_status: 7 },
        { dispatch_status_code: "I004", status: "CL-HOLD ", remarks: "Client Hold ", dispatch_status: 7 },
        { dispatch_status_code: "I005", status: "CL-SELFCOLLECT ", remarks: "Client Self Collect ", dispatch_status: 7 },
        { dispatch_status_code: "I006", status: "SD-REATTEMPT ", remarks: "Shipdelight Reattempt ", dispatch_status: 7 },
        { dispatch_status_code: "I007", status: "SD-RTO-INITIATED ", remarks: "sd rto initiated ", dispatch_status: 7 },
        { dispatch_status_code: "I008", status: "SD-SELFCOLLECT ", remarks: "Shipdelight Self Collect ", dispatch_status: 7 },
        { dispatch_status_code: "N001", status: "Whatsapp Calling ", remarks: "Whatsapp Calling ", dispatch_status: 8 },
        { dispatch_status_code: 402, status: "Partial Delivered ", remarks: "Partial Delivered ", dispatch_status: 21 },
        { dispatch_status_code: "R1207", status: "Reverse UNDELIVERED ", remarks: "on hold ", dispatch_status: 6 },
        { dispatch_status_code: "R1208", status: "Reverse UNDELIVERED ", remarks: "vendor not available ", dispatch_status: 6 },
      ]

      if (track_parent_status_code != '' && track_parent_status_code == '6') {
        var result = masters.find(c => c.dispatch_status == status_dispatch_status && c.dispatch_status_code == status_dispatch_status_code)
        if (result != undefined) {
          // console.log("result---",result.remarks)
          var latest_undelivered_status_remark = result.remarks;
        } else {
          var latest_undelivered_status_remark = '';
        }

      } else {
        latest_undelivered_status_remark = '';
      }


      await orderGenerate.push({
        sync_source: sync_source,
        consignorname: consignorname,
        consignorsub_vendorname: consignorsub_vendorname,
        orderno: orderno,
        airwaybilno: airwaybilno,
        status_rto_tracking_no: status_rto_tracking_no,
        import_date: import_date,
        pickup_date: pickup_date,
        courier_code: courier_code,
        track_location: track_location,
        latest_status: latest_status,
        last_undel_reason: last_undel_reason,
        ndr_action: ndr_action,
        ndr_action_date: ndr_action_date,
        latest_undelivered_status_remark: latest_undelivered_status_remark,
        status_remark: status_remark,
        consigneefirstname: consigneefirstname,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneestate: consigneestate,
        consigneepincode: consigneepincode,
        consigneetelephone1: consigneetelephone1,
        payment_paytype: package_product,
        packages_description: package_item_description,
        packages_quantity: package_pieces,
        packages_actual_weight: package_actual_weight,
        packages_volumetric_weight: package_volumetric_weight,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorcity: consignorsub_vendorcity,
        lost_damage_date: lost_damage_date,
        delivery_date: delivery_date,
        initiated_date: initiated_date,
        intransit_date: intransit_date,
        rto_delivery_date: rto_delivery_date,
        no_of_attempt: no_of_attempt,
        last_attempt_date: last_attempt_date,
        delivery_tat: deliverytat,
        payment_codpayment_paiddate: payments_codpayment_paiddate,
        payment_codpayment_cod_paid: payments_codpayment_cod_paid,
        payment_codpayment_paidbankrefno: payments_codpayment_paidbankrefno,
        payment_codcollect_cod_recived: payments_codcollect_cod_recived,
        status_zone_code: status_zone_code,
        first_ofd_date: first_ofd_date,
        sec_ofd_date: sec_ofd_date,
        third_ofd_date: third_ofd_date,
        last_ofd_date: last_ofd_date,
        weightupdate_actual_weight: weightupdate_actual_weight,
        order_type: order_type,

      })

    }

    const OrdersV2 = await ordersV2Model.find({
      "status.booking_date": {
        "$gte": startDate + ' 00:00:00',
        "$lte": endDate + ' 23:59:59'
      },
      $or: [
        {
          "status.dispatch_status": {
            "$gte": "0",
          }
        },
        {
          "status.dispatch_status": '0'
        },
        {
          'airwaybilno': {
            "$ne": 'null'
          }
        },
      ],
    });
    console.log("OrdersV2---", OrdersV2.length)
    for await (let data of OrdersV2) {
      // console.log("data---",data.airwaybilno)
      var airwaybilno = data.airwaybilno ? data.airwaybilno : null;
      var api_serviceable_list = api_serviceable_list ? api_serviceable_list : null
      var orderno = data.orderno ? data.orderno : null;
      var sync_source = data.sync_source ? data.sync_source : 'V2';
      if (data.consignee) {
        var consigneeaddress1 = data.consignee.address1 ? data.consignee.address1 : null;
        var consigneecity = data.consignee.city ? data.consignee.city : null;
        var consigneefirstname = data.consignee.firstname ? data.consignee.firstname : null;
        var consigneepincode = data.consignee.pincode ? data.consignee.pincode : null;
        var consigneestate = data.consignee.state ? data.consignee.state : null;
        var consigneetelephone1 = data.consignee.telephone1 ? data.consignee.telephone1 : null;
        var consigneeaddress2 = data.consignee.address2 ? data.consignee.address2 : null;
      }
      if (data.consignor) {
        var consignorname = data.consignor.name ? data.consignor.name : null;
        var consignorrto = data.consignor.rto ? data.consignor.rto : null;
        var consignorsub_vendor = data.consignor.sub_vendor ? data.consignor.sub_vendor : null;
        if (consignorsub_vendor) {
          var consignorsub_vendorcity = data.consignor.sub_vendor.city ? data.consignor.sub_vendor.city : null;
          var consignorsub_vendorname = data.consignor.sub_vendor.name ? data.consignor.sub_vendor.name : null;
          var consignorsub_vendorphone = data.consignor.sub_vendor.phone ? data.consignor.sub_vendor.phone : null;
          var consignorsub_vendorpincode = data.consignor.sub_vendor.pincode ? data.consignor.sub_vendor.pincode : null;

        }

      }
      if (data.courier) {
        var courier_code = data.courier.courier_code ? data.courier.courier_code : null;
      }
      if (data.payment) {
        var payment_paytype = data.payment.paytype ? data.payment.paytype : null;
      }
      if (data.status) {
        var status_booking_date = data.status.booking_date ? data.status.booking_date : null;
        var status_dispatch_status = data.status.dispatch_status ? data.status.dispatch_status : null;
        var status_dispatch_status_code = data.status.dispatch_status_code ? data.status.dispatch_status_code : null;
        var status_last_attempt_date = data.status.last_attempt_date ? data.status.last_attempt_date : null;
        var status_pickup_date = data.status.pickup_date ? data.status.pickup_date : null;
        var status_delivery_date = data.status.delivery_date ? data.status.delivery_date : null;
        var status_rto_delivery_date = data.status.rto_delivery_date ? data.status.rto_delivery_date : null;
        var status_rto_initiated_date = data.status.rto_initiated_date ? data.status.rto_initiated_date : null;
        var status_rto_intransit_date = data.status.rto_intransit_date ? data.status.rto_intransit_date : null;
        var status_rto_tracking_no = data.status.rto_tracking_no ? data.status.rto_tracking_no : null;
        var status_zone_code = data.status.zone_code ? data.status.zone_code : null;
        var status_import_date = data.status.import_date ? data.status.import_date : null;
        if (status_delivery_date) {
          var delivery_date = moment(status_delivery_date).format('YYYY-MM-DD');
        } else {
          var delivery_date = ''
        }
        if (status_last_attempt_date) {
          var last_attempt_date = moment(status_last_attempt_date).format('YYYY-MM-DD');
        } else {
          var last_attempt_date = '';
        }
        if (status_booking_date) {
          var import_date = moment(status_booking_date).format('YYYY-MM-DD');
        } else {
          var import_date = '';
        }
        if (status_rto_initiated_date) {
          var initiated_date = moment(status_rto_initiated_date).format('YYYY-MM-DD');
        } else {
          var initiated_date = '';
        }
        if (status_rto_intransit_date) {
          var intransit_date = moment(status_rto_intransit_date).format('YYYY-MM-DD');
        } else {
          var intransit_date = '';
        }
        if (status_rto_delivery_date) {
          var rto_delivery_date = moment(status_rto_delivery_date).format('YYYY-MM-DD');
        } else {
          var rto_delivery_date = '';
        }

        if (status_delivery_date && status_delivery_date != "0000-00-00 00:00:00" && status_delivery_date != "" && status_pickup_date && status_pickup_date != "0000-00-00 00:00:00" && status_pickup_date != "") {
          const date1 = new Date(status_pickup_date);
          const date2 = new Date(status_delivery_date);
          const diffTime = Math.abs(date2 - date1);
          const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
          // console.log(diffTime + " milliseconds");
          var deliverytat = diffDays
          // console.log("deliverytat@@---",deliverytat + " days");
        } else {
          var deliverytat = "";
          // console.log("deliverytat----",deliverytat)
        }
        if (deliverytat <= "6") {
          var tat = 'Day ' + deliverytat
          //  console.log("tat1---",tat)
        } else if (deliverytat > 6 && deliverytat < 16) {
          var tat = '7 To 15 Days';
          //  console.log("tat2---",tat)
        } else if (deliverytat > 15 && deliverytat < 31) {
          var tat = '16 To 30 Days';
          //  console.log("tat3---",tat)
        } else if (deliverytat > 30) {
          var tat = '>30 Days';
          //  console.log("tat4---",tat)
        } else {
          var tat = '';
          // console.log("tat5---",tat)
        }

      }
      if (data.weight_update) {
        var weightupdate_actual_weight = data.weight_update.actual_weight ? data.weight_update.actual_weight : null;
      }

      var packages = data.packages ? data.packages : [];
      if (packages != '') {
        for (let pack of packages) {
          var packages_description = pack.description ? pack.description : null;
          // var packages_sku = pack.sku ? pack.sku : null;
          var packages_quantity = pack.quantity ? pack.quantity : null;
          var packages_actual_weight = pack.actual_weight ? pack.actual_weight : null;
          var package_volumetric_weight = pack.volumetric_weight ? pack.volumetric_weight : null;
          // var packages_length = pack.length ? pack.length : null;
          // var packages_breadth = pack.breadth ? pack.breadth : null;
          // var packages_height = pack.height ? pack.height : null;
          var packages_price = pack.price ? pack.price : null;

          var total_quantity = 0, total_weight = 0, total_price = 0;
          // console.log("packages_quantity---",packages_quantity)
          total_price += (packages_price != '' ? parseFloat(packages_price) : 0);//product_value
          total_quantity += !isNumeric(packages_quantity) ? 1 : packages_quantity;
          total_weight += !isNumeric(packages_actual_weight) ? 0 : packages_actual_weight;
          // console.log("total_price---",total_price)
        }

      }
      var tracking = data.tracking ? data.tracking : [];
      // console.log("tracking---",tracking)

      var first_ofd_date = sec_ofd_date = third_ofd_date = last_ofd_date = '';
      var i = 0;
      var count = 0;
      if (tracking != '') {
        // console.log("tracking---",tracking)
        var statuscode = '305';
        var no_of_attempt = 0;
        var tracking_counts = [];
        var obj = {}
        tracking.forEach(function (item) {
          obj[item.status_code] ? obj[item.status_code]++ : obj[item.status_code] = 1;
        });
        // console.log("obj----",obj)  
        // console.log("count---",obj[statuscode])
        no_of_attempt = obj[statuscode];
        // console.log("tracking[0]----",tracking[0])
        for await (let track of tracking) {
          // var track_lsp_status_code = track.lsp_status_code ? track.lsp_status_code : null;
          var track_status = track.status ? track.status : 'PENDING PICKUP';
          var track_status_code = track.status_code ? track.status_code : null;
          // var track_lsp_status = track.lsp_status ? track.lsp_status : null;
          var track_parent_status_code = track.parent_status_code ? track.parent_status_code : null;
          // var track_source_status_code = track.source_status_code ? track.source_status_code : null;
          var track_remarks = track.remarks ? track.remarks : null;
          var track_updated_date = track.updated_date ? track.updated_date : null;
          var track_location = track.location ? track.location : null;
          // var track_manualentry = track.manualentry ? track.manualentry : null;
          // console.log("track_status_code.length----",track_status_code.length)



          // console.log("track_status_code--",track_status_code)
          if (track_status_code == '305') {
            // console.log("reached hear")
            if (i == 0) {
              var first_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("first_ofd_date----",first_ofd_date)
            }
            if (i == 1) {
              var sec_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("sec_ofd_date----",sec_ofd_date)
            }
            if (i == 2) {
              var third_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
              // console.log("third_ofd_date----",third_ofd_date)
            }
            var last_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
            i++;
            // console.log("i---",i)
          }
          else {
            var first_ofd_date = first_ofd_date;
            var sec_ofd_date = sec_ofd_date;
            var third_ofd_date = third_ofd_date;
            var last_ofd_date = last_ofd_date;
          }

          // if(track_parent_status_code =="4") //OUT FOR DEL
          // {            
          //     last_ofd_date = moment(track_updated_date).format('YYYY-MM-DD');
          // }else{
          //   last_ofd_date = '';
          // }
        }
        // console.log("track_parent_status_code---",track_parent_status_code)
        count = tracking.length;
        if (count > 0) {
          count = count - 1;
          var status_remark = tracking[count]['remarks'];
        } else {
          var status_remark = 'PENDING PICKUP';
        }

        // console.log("track_status_code---",track_status_code)
        if (track_status_code == "I002" || track_status_code == "I003" || track_status_code == "I006" || track_status_code == "I007") {
          // latest_status = 'UNDELIVERED';
          ndr_action = tracking[count]['remarks'];
          ndr_action_date = moment(tracking[count]['track_updated_date']).format('YYYY-MM-DD');
          // console.log("ndr_action---",ndr_action)
          // console.log("ndr_action_date---",ndr_action_date)
        } else if (status_dispatch_status == "18") {
          ndr_action = 'CL-REATTEMPT';
          ndr_action_date = '';
        } else {
          ndr_action = '';
          ndr_action_date = '';
        }
      }
      var latest_status = '';
      if (status_dispatch_status == '0') {
        latest_status = "PENDING PICKUP";
      } else if (status_dispatch_status == '1') {
        latest_status = "PICKUP DONE";
      } else if (status_dispatch_status == '2') {
        latest_status = "IN-TRANSIT";
      } else if (status_dispatch_status == '4') {
        latest_status = "OUT FOR DELIVERY"
      } else if (status_dispatch_status == '5') {
        latest_status = "DELIVERED"
      } else if (status_dispatch_status == '6') {
        latest_status = "UNDELIVERED"
      } else if (status_dispatch_status == '7') {
        latest_status = "UNDELIVERED"
      } else if (status_dispatch_status == '10') {
        latest_status = "RTO INITIATED"
      } else if (status_dispatch_status == '11') {
        latest_status = "RTO IN-TRANSIT"
      } else if (status_dispatch_status == '12') {
        latest_status = "RTO OUT FOR DELIVERY"
      } else if (status_dispatch_status == '13') {
        latest_status = "RTO DELIVERED"
      } else if (status_dispatch_status == '14') {
        latest_status = "PICKUP CANCEL BY CLIENT"
      } else if (status_dispatch_status == '15') {
        latest_status = "LOST"
      } else if (status_dispatch_status == '16') {
        latest_status = "SHIPMENT DAMAGE"
      } else if (status_dispatch_status == '17') {
        latest_status = "DANGER GOODS"
      } else if (status_dispatch_status == '18') {
        latest_status = "REATTEMPT"
      } else if (status_dispatch_status == '19') {
        latest_status = "RTO"
      } else if (status_dispatch_status == '20') {
        latest_status = "RTO"
      } else if (status_dispatch_status == '21') {
        latest_status = "SELF COLLECT (CS)"
      } else if (status_dispatch_status == '55') {
        latest_status = "RTO UNDELIVERED"
      } else if (status_dispatch_status == '56') {
        latest_status = "REVERSE UNDELIVERED"
      } else if (status_dispatch_status == '1001') {
        latest_status = "UNATTEMPTED"
      } else if (status_dispatch_status == '8') {
        latest_status = "UNDELIVERED"
      } else {
        latest_status = "PENDING PICKUP";
        // status_remark="PENDING PICKUP";
      }

      var latest_status_date = track_updated_date;
      var latest_status_code = '99';
      if (track_status_code) {
        var latest_status_code = track_status_code;
      } else {
        var latest_status_code = '';
      }
      var latest_parent_status_code = '0';
      if (track_parent_status_code) {
        latest_parent_status_code = track_parent_status_code;
      } else {
        latest_parent_status_code = '';
      }

      // console.log("track_status_code---",track_status_code)


      // console.log("track_parent_status_code---",track_parent_status_code)
      var status_remark, last_undel_reason, ndr_action, ndr_action_date = ''
      // console.log("track_parent_status_code---",track_parent_status_code)
      // if ((track_parent_status_code == "10" || track_parent_status_code == "18")) {
      //   status_remark = track_remarks;
      //   // console.log("status_remark---",status_remark)
      // } else {
      //   status_remark = '';
      // }
      // console.log("track_parent_status_code---",track_parent_status_code)
      if (track_parent_status_code != '' && (track_parent_status_code == "6" || track_parent_status_code == "55")) {
        last_undel_reason = track_remarks;
        // console.log("last_undel_reason---",last_undel_reason)
      } else {
        last_undel_reason = '';
      }

      // console.log("latest_status_code---",latest_status_code)
      if (latest_status_code && (latest_status_code == "I002" || latest_status_code == "I003" || latest_status_code == "I004" || latest_status_code == "I005" || latest_status_code == "I006" || latest_status_code == "I007")) {
        latest_status = 'UNDELIVERED';
      }


      // console.log("track_parent_status_code---",track_parent_status_code)
      if (status_pickup_date && latest_status && track_parent_status_code && track_parent_status_code != '14') {
        var pickup_date = moment(status_pickup_date).format('YYYY-MM-DD');
      } else {
        var pickup_date = '';
      }
      var lost_damage_date = ''
      if (track_status_code && (track_status_code == '901' || track_status_code == '902') && track_updated_date && track_updated_date != '') {
        lost_damage_date = moment(track_updated_date).format('YYYY-MM-DD');
        // console.log("lost_damage_date---",lost_damage_date)
      } else {
        lost_damage_date = "";
        // console.log("lost_damage_date@@---",lost_damage_date)
      }

      var order_type = "FORWARD";
      //  console.log("package_rp---",package_rp)
      // if (package_rp && package_rp == 1) {
      //   var order_type = 'REVERSE';
      //   // console.log("order_type---",order_type)
      // }

      const masters = [
        { dispatch_status_code: 99, status: "PENDING PICKUP", remarks: "pending pickup", dispatch_status: 0 },
        { dispatch_status_code: 100, status: "PICKUP DONE", remarks: "pickup done", dispatch_status: 1 },
        { dispatch_status_code: 102, status: "IN-TRANSIT", remarks: "processing at origin hub", dispatch_status: 2 },
        { dispatch_status_code: 305, status: "OUT FOR DELIVERY", remarks: "out for delivery", dispatch_status: 4 },
        { dispatch_status_code: 400, status: "Delivered", remarks: "delivered", dispatch_status: 5 },
        { dispatch_status_code: 401, status: "RTO Delivered", remarks: "rto delivered", dispatch_status: 13 },
        { dispatch_status_code: 500, status: "UNDELIVERED", remarks: "consignee refused", dispatch_status: 6 },
        { dispatch_status_code: 501, status: "UNDELIVERED", remarks: "incomplete address", dispatch_status: 6 },
        { dispatch_status_code: 503, status: "UNDELIVERED", remarks: "oda", dispatch_status: 6 },
        { dispatch_status_code: 504, status: "UNDELIVERED", remarks: "consignee shifted", dispatch_status: 6 },
        { dispatch_status_code: 505, status: "UNDELIVERED", remarks: "DAMAGED", dispatch_status: 6 },
        { dispatch_status_code: 506, status: "UNDELIVERED", remarks: "no such consignee", dispatch_status: 6 },
        { dispatch_status_code: 507, status: "UNDELIVERED", remarks: "future delivery", dispatch_status: 6 },
        { dispatch_status_code: 508, status: "UNDELIVERED", remarks: "cod not ready", dispatch_status: 6 },
        { dispatch_status_code: 509, status: "UNDELIVERED", remarks: "residence/office closed", dispatch_status: 6 },
        { dispatch_status_code: 510, status: "UNDELIVERED", remarks: "out of station", dispatch_status: 6 },
        { dispatch_status_code: 511, status: "UNDELIVERED", remarks: "shipment lost", dispatch_status: 6 },
        { dispatch_status_code: 512, status: "UNDELIVERED", remarks: "Dangerous Goods", dispatch_status: 6 },
        { dispatch_status_code: 513, status: "UNDELIVERED", remarks: "Self Collect", dispatch_status: 6 },
        { dispatch_status_code: 514, status: "UNDELIVERED", remarks: "Held With Govt Authority", dispatch_status: 6 },
        { dispatch_status_code: 515, status: "UNDELIVERED", remarks: "consignee not available", dispatch_status: 6 },
        { dispatch_status_code: 516, status: "UNDELIVERED", remarks: "consignee not responding", dispatch_status: 6 },
        { dispatch_status_code: 517, status: "UNDELIVERED", remarks: "misroute", dispatch_status: 6 },
        { dispatch_status_code: 518, status: "UNDELIVERED", remarks: "on hold", dispatch_status: 6 },
        { dispatch_status_code: 519, status: "UNDELIVERED", remarks: "restricted area", dispatch_status: 6 },
        { dispatch_status_code: 520, status: "UNDELIVERED", remarks: "snatched by consignee", dispatch_status: 6 },
        { dispatch_status_code: 521, status: "UNDELIVERED", remarks: "disturbance/natural disaster/strike/COVID", dispatch_status: 6 },
        { dispatch_status_code: 522, status: "UNDELIVERED", remarks: "Open Delivery", dispatch_status: 6 },
        { dispatch_status_code: 523, status: "UNDELIVERED", remarks: "Customer denied - OTP Delivery", dispatch_status: 6 },
        { dispatch_status_code: 524, status: "UNDELIVERED", remarks: "Time Constraint / Dispute", dispatch_status: 6 },
        { dispatch_status_code: 600, status: "RTO INITIATED", remarks: "rto initiated ", dispatch_status: 10 },
        { dispatch_status_code: 601, status: "RTO IN-TRANSIT", remarks: "rto intransit ", dispatch_status: 11 },
        { dispatch_status_code: 615, status: "RTO UNDELIVERED ", remarks: "Vendor refused ", dispatch_status: 55 },
        { dispatch_status_code: 900, status: "Pickup Cancelled ", remarks: "Pickup Cancelled by Client ", dispatch_status: 14 },
        { dispatch_status_code: 901, status: "LOST ", remarks: "SHIPMENT LOST ", dispatch_status: 15 },
        { dispatch_status_code: 902, status: "SHIPMENT DAMAGE ", remarks: "SHIPMENT DAMAGE ", dispatch_status: 16 },
        { dispatch_status_code: 951, status: "REATTEMPT ", remarks: "Reattempt ", dispatch_status: 18 },
        { dispatch_status_code: 1001, status: "Unattempted ", remarks: "Unattempted ", dispatch_status: 1001 },
        { dispatch_status_code: "I001", status: "SWC ", remarks: "Shared with Client ", dispatch_status: 7 },
        { dispatch_status_code: "I002", status: "CL-REATTEMPT ", remarks: "Client Reattempt ", dispatch_status: 7 },
        { dispatch_status_code: "I003", status: "CL-RTO-INITIATED ", remarks: "cl rto initiated ", dispatch_status: 7 },
        { dispatch_status_code: "I004", status: "CL-HOLD ", remarks: "Client Hold ", dispatch_status: 7 },
        { dispatch_status_code: "I005", status: "CL-SELFCOLLECT ", remarks: "Client Self Collect ", dispatch_status: 7 },
        { dispatch_status_code: "I006", status: "SD-REATTEMPT ", remarks: "Shipdelight Reattempt ", dispatch_status: 7 },
        { dispatch_status_code: "I007", status: "SD-RTO-INITIATED ", remarks: "sd rto initiated ", dispatch_status: 7 },
        { dispatch_status_code: "I008", status: "SD-SELFCOLLECT ", remarks: "Shipdelight Self Collect ", dispatch_status: 7 },
        { dispatch_status_code: "N001", status: "Whatsapp Calling ", remarks: "Whatsapp Calling ", dispatch_status: 8 },
        { dispatch_status_code: 402, status: "Partial Delivered ", remarks: "Partial Delivered ", dispatch_status: 21 },
        { dispatch_status_code: "R1207", status: "Reverse UNDELIVERED ", remarks: "on hold ", dispatch_status: 6 },
        { dispatch_status_code: "R1208", status: "Reverse UNDELIVERED ", remarks: "vendor not available ", dispatch_status: 6 },
      ]
      // console.log("track_parent_status_code---",track_parent_status_code)
      var latest_undelivered_status_remark = '';
      if (track_parent_status_code != '' && track_parent_status_code == '6') {
        var result = masters.find(c => c.dispatch_status == status_dispatch_status && c.dispatch_status_code == status_dispatch_status_code)
        if (result != undefined) {
          // console.log("result---",result.remarks)
          latest_undelivered_status_remark = result.remarks;
          // console.log("latest_undelivered_status_remark---",latest_undelivered_status_remark)
        } else {
          latest_undelivered_status_remark = '';
          // console.log("latest_undelivered_status_remark@@---",latest_undelivered_status_remark)
        }

      } else {
        latest_undelivered_status_remark = '';
      }
      await orderGenerate.push({
        sync_source: sync_source,
        consignorname: consignorname,
        consignorsub_vendorname: consignorsub_vendorname,
        orderno: orderno,
        airwaybilno: airwaybilno,
        status_rto_tracking_no: status_rto_tracking_no,
        import_date: import_date,
        pickup_date: pickup_date,
        courier_code: courier_code,
        track_location: track_location,
        latest_status: latest_status,
        last_undel_reason: last_undel_reason,
        ndr_action: ndr_action,
        ndr_action_date: ndr_action_date,
        latest_undelivered_status_remark: latest_undelivered_status_remark,
        status_remark: status_remark,
        consigneefirstname: consigneefirstname,
        consigneeaddress1: consigneeaddress1,
        consigneeaddress2: consigneeaddress2,
        consigneecity: consigneecity,
        consigneestate: consigneestate,
        consigneepincode: consigneepincode,
        consigneetelephone1: consigneetelephone1,
        payment_paytype: payment_paytype,
        packages_description: packages_description,
        packages_quantity: total_quantity,
        packages_actual_weight: total_weight,
        packages_volumetric_weight: package_volumetric_weight,
        consignorsub_vendorphone: consignorsub_vendorphone,
        consignorsub_vendorpincode: consignorsub_vendorpincode,
        consignorsub_vendorcity: consignorsub_vendorcity,
        lost_damage_date: lost_damage_date,
        delivery_date: delivery_date,
        initiated_date: initiated_date,
        intransit_date: intransit_date,
        rto_delivery_date: rto_delivery_date,
        no_of_attempt: no_of_attempt,
        last_attempt_date: last_attempt_date,
        delivery_tat: deliverytat,
        payment_codpayment_paiddate: payments_codpayment_paiddate,
        payment_codpayment_cod_paid: payments_codpayment_cod_paid,
        payment_codpayment_paidbankrefno: payments_codpayment_paidbankrefno,
        payment_codcollect_cod_recived: payments_codcollect_cod_recived,
        status_zone_code: status_zone_code,
        first_ofd_date: first_ofd_date,
        sec_ofd_date: sec_ofd_date,
        third_ofd_date: third_ofd_date,
        last_ofd_date: last_ofd_date,
        weightupdate_actual_weight: weightupdate_actual_weight,
        order_type: order_type,

      })
    }


    var date = moment(new Date()).format('yyyy-MM-DD');
    var hostname = req.headers.host;
    var currentPath = process.cwd();
    var dir = currentPath + '/src/public/Order_Generated_Reports/';
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }
    var fileName = 'Order_Generated_Report_' + startDate + '_' + endDate + '.csv';

    const csvWriter = createCsvWriter({

      // Output csv file name is geek_data
      path: dir + fileName,
      options:
        { flags: 'a' },
      header: [

        // Title of the columns (column_names)
        { id: 'sync_source', title: 'Data Source' },
        { id: 'consignorname', title: 'Client Name' },
        { id: 'consignorsub_vendorname', title: 'Sub Vendor Name' },
        { id: 'orderno', title: 'Order No' },
        { id: 'airwaybilno', title: 'AirwayBill No' },
        { id: 'status_rto_tracking_no', title: 'RTO No' },
        { id: 'import_date', title: 'Import Date' },
        { id: 'pickup_date', title: 'Pickup Date' },
        { id: 'courier_code', title: 'Courier Name' },
        { id: 'track_location', title: 'Location' },
        { id: 'latest_status', title: 'Latest Status' },
        { id: 'last_undel_reason', title: 'Status Remark' },
        { id: 'ndr_action', title: 'NDR Action' },
        { id: 'latest_undelivered_status_remark', title: 'Last Undelivered Reason' },
        { id: 'status_remark', title: 'Last Courier Reason' },
        { id: 'consigneefirstname', title: 'Consignee Firstname' },
        { id: 'consigneeaddress1', title: 'Consignee Address1' },
        { id: 'consigneeaddress2', title: 'Consignee Address2' },
        { id: 'consigneecity', title: 'Destination City' },
        { id: 'consigneestate', title: 'State' },
        { id: 'consigneepincode', title: 'Pincode' },
        { id: 'consigneetelephone1', title: 'Consignee Phone1' },
        { id: 'payment_paytype', title: 'Pay Type' },
        { id: 'packages_description', title: 'Package Title' },
        // { id: 'packages_price', title: 'Price' },
        { id: 'packages_quantity', title: 'Quantity' },
        { id: 'packages_actual_weight', title: 'Actual Weight' },
        { id: 'packages_volumetric_weight', title: 'Volumetric Weight' },
        { id: 'consignorsub_vendorphone', title: 'Sub Vendor Contact' },
        { id: 'consignorsub_vendorcity', title: 'Sub Vendor Pickup City' },
        { id: 'consignorsub_vendorpincode', title: 'Sub Vendor Pincode' },
        { id: 'lost_damage_date', title: 'Lost/Damage Date' },
        { id: 'delivery_date', title: 'Delivery Date' },
        { id: 'ndr_action_date', title: 'Action Date' },
        { id: 'initiated_date', title: 'RTO Initiated Date' },
        { id: 'intransit_date', title: 'RTO_Intransit Date' },
        { id: 'rto_delivery_date', title: 'RTO Delivery Date' },
        { id: 'no_of_attempt', title: 'No Of Attempt' },
        { id: 'last_attempt_date', title: 'Last Scan Date' },
        { id: 'delivery_tat', title: 'Delivery TAT' },
        { id: 'payment_codpayment_paiddate', title: 'COD Payment Paid Date' },
        { id: 'payment_codpayment_cod_paid', title: 'Amount Paid' },
        { id: 'payment_codpayment_paidbankrefno', title: 'Client Bank Ref No' },
        { id: 'payment_codcollect_cod_recived', title: 'Recived Paid' },
        { id: 'status_zone_code', title: 'Zone' },
        { id: 'first_ofd_date', title: '1st OFD Date' },
        { id: 'sec_ofd_date', title: '2nd OFD Date' },
        { id: 'third_ofd_date', title: '3rd OFD Date' },
        { id: 'last_ofd_date', title: 'Last OFD Date' },
        { id: 'weightupdate_actual_weight', title: 'Updated Weight' },
        { id: 'order_type', title: 'Order type' },

      ]
    });
    csvWriter
      .writeRecords(orderGenerate)
      .then(() => console.log('Data uploaded into csv successfully'));
    res.json({
      status: 200,
      data: 'http://' + hostname + '/Order_Generated_Reports/' + fileName
    })

  } catch (error) {
    console.log(`Error: ${error.message}`)
    return res.status(400).json({
      message: 'Bad Request',
    })
  }

})

/*
    Add cron for delete current date previous 7 days csv files. (Date: 12-01-2022)
*/
cron.schedule('00 00 12 * * 0-6', async () => {
  var currentPath = process.cwd();
  // console.log("currentPath---",currentPath)
  var publicDir = currentPath + '/src/public'
  fs.readdir(publicDir, function (err, files) {
    // console.log("files---",files)
    if (files) {
      files.forEach(function (file, index) {
        // console.log("file---",file)
        fs.readdir(publicDir + '/' + file, function (err, subfile) {
          // console.log("subfile---",subfile)
          if (subfile) {
            subfile.forEach(function (sub, index) {
              // console.log("sub---",sub)
              fs.stat(path.join(publicDir + '/' + file, sub), function (err, stat) {
                var endTime, now;
                if (err) {
                  return console.error(err);
                }
                now = new Date().getTime();
                endTime = new Date(stat.ctime).getTime() + 604800000;
                // console.log("ctime----",new Date(stat.ctime).getTime())
                // console.log("now----",now)
                // console.log("endTime----",endTime)
                if (now > endTime) {
                  return rimraf(path.join(publicDir + '/' + file, sub), function (err) {
                    if (err) {
                      return console.error(err);
                    }
                    // console.log('successfully deleted');
                  });
                }
              });
            })
          }

        })

      });
    }

  });


})

router.get("/download", function (req, res) {
  // var location = req.query.file;
  var location = 'src/public/Intransit_Reports/Intransit_Report_2022-02-15.csv'
  const downloadData = location;
  res.download(downloadData);
});


module.exports = router;