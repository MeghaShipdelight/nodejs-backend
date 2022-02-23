
var mongoose = require('mongoose');
var conn2 = mongoose.createConnection('mongodb://admin:Qwerty123@164.52.208.159:27017/instashipin?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false');
var OrdersV2Schema = new mongoose.Schema({
  airwaybilno :{type: String,required: true},
    consignee: {type:Object,required:true},
    consignor: {type:Object,required:true},
    courier: {type:Object,required:true},
    ndr_status: {type:Object,required:true},
    orderno :{type: String ,required: true},
    packages : {type: Array,required: true},
    payment: {type:Object,required:true},
    status: {type:Object,required:true},
    tenant_id : {type: Number,required: true},
    tracking: {type: Array,required: true},
    transaction_id: {type: Number,required: true},
    updated_at: {type:Date,required: true},
    user_visited:{type: Number,required: true},
    website: {type:Object ,required:true},
    created_at:{type:Date,required: true},
    product_description: {type:String,required: true},
    servicetype: {type:String,required: true},
    channel: {type:Object ,required:true},
    ndr_ticket:{type:Array,required: true},
    customer_feedback: {type:String,required: true},
    feedback: {type:String,required: true},
    consignor_billing: {type:String,required: true},
    consignor_billing_status: {type:Object ,required:true},
    sync_source: {type:String,required: true},
    ndr_attempt: {type:Array,required: true},
    weight_update: {type:Object ,required:true},
    // package: {type:Object ,required:true},
    payments: {type:Object ,required:true},
    api_servicable_list: {type:String ,required:true},
    success: {type:String ,required:true},
})

module.exports = conn2.model('Orders', OrdersV2Schema);