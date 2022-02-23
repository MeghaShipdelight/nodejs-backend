var mongoose = require('mongoose');
var Schema = mongoose.Schema;
var OPRILogixSchema = new Schema({
    datasource :{type: String,required: true},
    clientname: {type:String,required:true},
    subvendorname: {type:String,required:true},
    orderno: {type:String,required:true},
    airwaybillno :{type: String ,required: true},
    rtono : {type: String,required: true},
    importdate: {type:String,required:true},
    pickupdate: {type:String,required:true},
    couriername : {type: String,required: true},
    location: {type: String,required: true},
    lateststatus: {type: String,required: true},
    lastcourierreason: {type: String,required: true},
    consigneefirstname:{type: String,required: true},
    consignee: {type:Object ,required:true},
    destination: {type:Object ,required:true},
    state: {type:String ,required:true},
    pincode: {type:Number ,required:true},
    cod: {type:Object ,required:true},
    product: {type:Object ,required:true},
    paytype: {type:String ,required:true},
    statusremark:{type:String, required:true},
    lastundeliveredreason:{type:String, required:true},
    quantity: {type:Number ,required:true},
    volumetricweight: {type:Number, required:true},
    sub: {type:Object ,required:true},
    delivery: {type:Object ,required:true},
    noofattempt: {type:Number ,required:true},
    lastscandate: {type:String,required:true},
    deliverytat: {type:Number ,required:true},
    zone: {type:String ,required:true},
    lastofddate: {type:String,required:true},
    ordertype: {type:String ,required:true},



});

mongoose.model('Logixs',OPRILogixSchema);