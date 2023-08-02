package org.rmj.cas.purchasing.base;

import com.mysql.jdbc.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;
import org.rmj.appdriver.constants.EditMode;
import org.rmj.appdriver.GCrypt;
import org.rmj.appdriver.GRider;
import org.rmj.appdriver.iface.GEntity;
import org.rmj.appdriver.iface.GTransaction;
import org.rmj.appdriver.MiscUtil;
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agentfx.ShowMessageFX;
import org.rmj.appdriver.agentfx.ui.showFXDialog;
import org.rmj.appdriver.constants.TransactionStatus;
import org.rmj.appdriver.constants.UserRight;
import org.rmj.cas.inventory.base.InventoryTrans;
import org.rmj.cas.inventory.base.InvExpiration;
import org.rmj.cas.inventory.base.Inventory;
import org.rmj.cas.purchasing.pojo.UnitPOReceivingDetail;
import org.rmj.cas.purchasing.pojo.UnitPOReceivingDetailOthers;
import org.rmj.cas.purchasing.pojo.UnitPOReceivingMaster;

public class POReceiving implements GTransaction{
    @Override
    public UnitPOReceivingMaster newTransaction() {
        UnitPOReceivingMaster loObj = new UnitPOReceivingMaster();
        Connection loConn = null;
        loConn = setConnection();
        
        loObj.setTransNox(MiscUtil.getNextCode(loObj.getTable(), "sTransNox", true, loConn, psBranchCd));
        
        //init detail
        poDetail = new ArrayList<>();
        paDetailOthers = new ArrayList<>();
        
        return loObj;
    }

    @Override
    public UnitPOReceivingMaster loadTransaction(String fsTransNox) {
        UnitPOReceivingMaster loObject = new UnitPOReceivingMaster();
        
        Connection loConn = null;
        loConn = setConnection();   
        
        String lsSQL = MiscUtil.addCondition(getSQ_Master(), "sTransNox = " + SQLUtil.toSQL(fsTransNox));
        System.out.println("POREceiving" + lsSQL);
        ResultSet loRS = poGRider.executeQuery(lsSQL);
        
        try {
            if (!loRS.next()){
                setMessage("No Record Found");
            }else{
                //load each column to the entity
                for(int lnCol=1; lnCol<=loRS.getMetaData().getColumnCount(); lnCol++){
                    loObject.setValue(lnCol, loRS.getObject(lnCol));
                }
                
                //load detail
                poDetail = loadTransDetail(fsTransNox);
                
            }              
        } catch (SQLException ex) {
            setErrMsg(ex.getMessage());
        } finally{
            MiscUtil.close(loRS);
            if (!pbWithParent) MiscUtil.close(loConn);
        }
        
        return loObject;
    }

    @Override
    public UnitPOReceivingMaster saveUpdate(Object foEntity, String fsTransNox) {
        String lsSQL = "";
        
        UnitPOReceivingMaster loOldEnt = null;
        UnitPOReceivingMaster loNewEnt = null;
        UnitPOReceivingMaster loResult = null;
        
        // Check for the value of foEntity
        if (!(foEntity instanceof UnitPOReceivingMaster)) {
            setErrMsg("Invalid Entity Passed as Parameter");
            return loResult;
        }
        
        // Typecast the Entity to this object
        loNewEnt = (UnitPOReceivingMaster) foEntity;
        
        
        // Test if entry is ok
        if (loNewEnt.getBranchCd()== null || loNewEnt.getBranchCd().isEmpty()){
            setMessage("Invalid branch detected.");
            return loResult;
        }
        
        if (loNewEnt.getDateTransact()== null){
            setMessage("Invalid transact date detected.");
            return loResult;
        }
        
        if (loNewEnt.getCompanyID()== null || loNewEnt.getCompanyID().isEmpty()){
            setMessage("Invalid company detected.");
            return loResult;
        }
        
        if (loNewEnt.getSupplier() == null || loNewEnt.getSupplier().isEmpty()){
            setMessage("Invalid supplier detected.");
            return loResult;
        }
               
        if (!pbWithParent) poGRider.beginTrans();
        
        if (poDetail.get(ItemCount()-1).getStockID().equals("")) deleteDetail(ItemCount() -1);
        
        if (ItemCount() <= 0){
            setMessage("Unable to save no item record.");
            return loResult;
        }
        
        // Generate the SQL Statement
        if (fsTransNox.equals("")){
            try {
                Connection loConn = null;
                loConn = setConnection();
                
                String lsTransNox = MiscUtil.getNextCode(loNewEnt.getTable(), "sTransNox", true, loConn, psBranchCd);
                
                loNewEnt.setTransNox(lsTransNox);
                loNewEnt.setModifiedBy(psUserIDxx);
                loNewEnt.setDateModified(poGRider.getServerDate());
                
                loNewEnt.setPreparedBy(psUserIDxx);
                loNewEnt.setDatePrepared(poGRider.getServerDate());
                
                //save detail first
                if(!saveDetail(loNewEnt, true)){
                    poGRider.rollbackTrans();
                    return null;
                }
                
                loNewEnt.setEntryNox(ItemCount());
                
                if (!pbWithParent) MiscUtil.close(loConn);
                
                //Generate the SQL Statement
                lsSQL = MiscUtil.makeSQL((GEntity) loNewEnt);
            } catch (SQLException ex) {
                Logger.getLogger(POReceiving.class.getName()).log(Level.SEVERE, null, ex);
            }
        }else{
            try {
                //Load previous transaction
                loOldEnt = loadTransaction(fsTransNox);
                
                //save detail first
                if(!saveDetail(loNewEnt, false)){
                    poGRider.rollbackTrans();
                    return null;
                }
                
                loNewEnt.setEntryNox(ItemCount());
                loNewEnt.setDateModified(poGRider.getServerDate());
                
                //Generate the Update Statement
                lsSQL = MiscUtil.makeSQL((GEntity) loNewEnt, (GEntity) loOldEnt, "sTransNox = " + SQLUtil.toSQL(loNewEnt.getTransNox()));
            } catch (SQLException ex) {
                Logger.getLogger(POReceiving.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        //No changes have been made
        if (lsSQL.equals("")){
            setMessage("Record is not updated");
            return loResult;
        }
        
        if(poGRider.executeQuery(lsSQL, loNewEnt.getTable(), "", "") == 0){
            if(!poGRider.getErrMsg().isEmpty())
                setErrMsg(poGRider.getErrMsg());
            else setMessage("No record updated");
        } else loResult = loNewEnt;
        
        if (!pbWithParent) {
            if (!getErrMsg().isEmpty()){
                poGRider.rollbackTrans();
            } else poGRider.commitTrans();
        }        
        
        return loResult;
    }

    @Override
    public boolean deleteTransaction(String fsTransNox) {
        UnitPOReceivingMaster loObject = loadTransaction(fsTransNox);
        boolean lbResult = false;
        
        if (loObject == null){
            setMessage("No record found...");
            return lbResult;
        }
        
        String lsSQL = "DELETE FROM " + loObject.getTable() + 
                        " WHERE sTransNox = " + SQLUtil.toSQL(fsTransNox);
        
        if (!pbWithParent) poGRider.beginTrans();
        
        if (poGRider.executeQuery(lsSQL, loObject.getTable(), "", "") == 0){
            if (!poGRider.getErrMsg().isEmpty()){
                setErrMsg(poGRider.getErrMsg());
            } else setErrMsg("No record deleted.");  
        } else lbResult = true;
        
        //delete detail rows
        lsSQL = "DELETE FROM " + pxeDetTable +
                " WHERE sTransNox = " + SQLUtil.toSQL(fsTransNox);
        
        if (poGRider.executeQuery(lsSQL, pxeDetTable, "", "") == 0){
            if (!poGRider.getErrMsg().isEmpty()){
                setErrMsg(poGRider.getErrMsg());
            } else setErrMsg("No record deleted.");  
        } else lbResult = true;
        
        if (!pbWithParent){
            if (getErrMsg().isEmpty()){
                poGRider.commitTrans();
            } else poGRider.rollbackTrans();
        }
        
        return lbResult;
    }

    public boolean closeTransaction(String fsTransNox, String fsApprovalCode) {
        UnitPOReceivingMaster loObject = loadTransaction(fsTransNox);
        boolean lbResult = false;
        
        if (loObject == null){
            setMessage("No record found...");
            return lbResult;
        }
        
        if (fsApprovalCode == null || fsApprovalCode.isEmpty()){
            setMessage("Invalid/No approval code detected.");
            return lbResult;
        }
        
        if (poGRider.getUserLevel() < UserRight.SUPERVISOR){
            setMessage("User is not allowed confirming transaction.");
            return lbResult;
        }
        
        
        if (!loObject.getTranStat().equalsIgnoreCase(TransactionStatus.STATE_OPEN)){
            setMessage("Unable to close closed/cancelled/posted/voided transaction.");
            return lbResult;
        }
        
        String lsSQL = "UPDATE " + loObject.getTable() + 
                        " SET  cTranStat = " + SQLUtil.toSQL(TransactionStatus.STATE_CLOSED) + 
                            ", sApproved = " + SQLUtil.toSQL(psUserIDxx) +
                            ", dApproved = " + SQLUtil.toSQL(poGRider.getServerDate()) + 
                            ", sAprvCode = " + SQLUtil.toSQL(fsApprovalCode) +
                            ", sModified = " + SQLUtil.toSQL(psUserIDxx) +
                            ", dModified = " + SQLUtil.toSQL(poGRider.getServerDate()) + 
                        " WHERE sTransNox = " + SQLUtil.toSQL(loObject.getTransNox());
        
        if (!pbWithParent) poGRider.beginTrans();
        
        if (poGRider.executeQuery(lsSQL, loObject.getTable(), "", "") == 0){
            if (!poGRider.getErrMsg().isEmpty()){
                setErrMsg(poGRider.getErrMsg());
            } else setErrMsg("No record deleted.");  
        } else lbResult = saveInvTrans(loObject.getTransNox(), loObject.getSupplier(),loObject.getDateTransact());
        
        if (!pbWithParent){
            if (getErrMsg().isEmpty()){
                poGRider.commitTrans();
            } else poGRider.rollbackTrans();
        }
        return lbResult;
    }

    @Override
    public boolean postTransaction(String fsTransNox) {
        if (poGRider.getUserLevel() <= UserRight.ENCODER){
            JSONObject loJSON = showFXDialog.getApproval(poGRider);
            
            if (loJSON == null){
                ShowMessageFX.Warning("Approval failed.", pxeModuleName, "Unable to post transaction");
            }
            
            if ((int) loJSON.get("nUserLevl") <= UserRight.ENCODER){
                ShowMessageFX.Warning("User account has no right to approve.", pxeModuleName, "Unable to post transaction");
                return false;
            }
        }

        UnitPOReceivingMaster loObject = loadTransaction(fsTransNox);
        boolean lbResult = false;
        
        if (loObject == null){
            setMessage("No record found...");
            return lbResult;
        }
        
        if (loObject.getTranStat().equalsIgnoreCase(TransactionStatus.STATE_POSTED) ||
            loObject.getTranStat().equalsIgnoreCase(TransactionStatus.STATE_CANCELLED) ||
            loObject.getTranStat().equalsIgnoreCase(TransactionStatus.STATE_VOID)){
            setMessage("Unable to post cancelled/posted/voided transaction.");
            return lbResult;
        }
        
        String lsSQL = "UPDATE " + loObject.getTable() + 
                        " SET  cTranStat = " + SQLUtil.toSQL(TransactionStatus.STATE_POSTED) + 
                            ", sPostedxx = " + SQLUtil.toSQL(psUserIDxx) +
                            ", dPostedxx = " + SQLUtil.toSQL(poGRider.getServerDate()) + 
                            ", sModified = " + SQLUtil.toSQL(psUserIDxx) +
                            ", dModified = " + SQLUtil.toSQL(poGRider.getServerDate()) + 
                        " WHERE sTransNox = " + SQLUtil.toSQL(loObject.getTransNox());
        
        if (!pbWithParent) poGRider.beginTrans();
        
        if (poGRider.executeQuery(lsSQL, loObject.getTable(), "", "") == 0){
            if (!poGRider.getErrMsg().isEmpty()){
                setErrMsg(poGRider.getErrMsg());
            } else setErrMsg("No record deleted.");  
        } else{
            lsSQL = "UPDATE PO_Master " + 
                        " SET  cTranStat = '4'"  + 
                            ", sPostedxx = " + SQLUtil.toSQL(psUserIDxx) +
                            ", dPostedxx = " + SQLUtil.toSQL(poGRider.getServerDate()) + 
                            ", sModified = " + SQLUtil.toSQL(psUserIDxx) +
                            ", dModified = " + SQLUtil.toSQL(poGRider.getServerDate()) + 
                        " WHERE sTransNox = " + SQLUtil.toSQL(loObject.getSourceNo());
            System.out.println(lsSQL);
            if (poGRider.executeQuery(lsSQL, "PO_Master", "", "") == 0){
                if (!poGRider.getErrMsg().isEmpty()){
                    setErrMsg(poGRider.getErrMsg());
                } else setErrMsg("No order updated.");  
            }else{
                lbResult = true;
            }
        }
        
        if (!pbWithParent){
            if (getErrMsg().isEmpty()){
                poGRider.commitTrans();
            } else poGRider.rollbackTrans();
        }
        return lbResult;
    }

    @Override
    public boolean voidTransaction(String fsTransNox) {
        UnitPOReceivingMaster loObject = loadTransaction(fsTransNox);
        boolean lbResult = false;
        
        if (loObject == null){
            setMessage("No record found...");
            return lbResult;
        }
        
        if (loObject.getTranStat().equalsIgnoreCase(TransactionStatus.STATE_POSTED)){
            setMessage("Unable to void posted transaction.");
            return lbResult;
        }
        
        String lsSQL = "UPDATE " + loObject.getTable() + 
                        " SET  cTranStat = " + SQLUtil.toSQL(TransactionStatus.STATE_VOID) + 
                            ", sModified = " + SQLUtil.toSQL(psUserIDxx) +
                            ", dModified = " + SQLUtil.toSQL(poGRider.getServerDate()) + 
                        " WHERE sTransNox = " + SQLUtil.toSQL(loObject.getTransNox());
        
        if (!pbWithParent) poGRider.beginTrans();
        
        if (poGRider.executeQuery(lsSQL, loObject.getTable(), "", "") == 0){
            if (!poGRider.getErrMsg().isEmpty()){
                setErrMsg(poGRider.getErrMsg());
            } else setErrMsg("No record deleted.");  
        } else lbResult = true;
        
        if (!pbWithParent){
            if (getErrMsg().isEmpty()){
                poGRider.commitTrans();
            } else poGRider.rollbackTrans();
        }
        return lbResult;
    }

    @Override
    public boolean cancelTransaction(String fsTransNox) {
        UnitPOReceivingMaster loObject = loadTransaction(fsTransNox);
        boolean lbResult = false;
        
        if (loObject == null){
            setMessage("No record found...");
            return lbResult;
        }
               
        if (loObject.getTranStat().equalsIgnoreCase(TransactionStatus.STATE_CANCELLED) ||
            loObject.getTranStat().equalsIgnoreCase(TransactionStatus.STATE_POSTED) ||
            loObject.getTranStat().equalsIgnoreCase(TransactionStatus.STATE_VOID)){
            setMessage("Unable to cancel cancelled/posted/voided transaction.");
            return lbResult;
        }
        
        String lsSQL = "UPDATE " + loObject.getTable() + 
                        " SET  cTranStat = " + SQLUtil.toSQL(TransactionStatus.STATE_CANCELLED) + 
                            ", dModified = " + SQLUtil.toSQL(poGRider.getServerDate()) + 
                        " WHERE sTransNox = " + SQLUtil.toSQL(loObject.getTransNox());
        
        if (!pbWithParent) poGRider.beginTrans();
        
        if (poGRider.executeQuery(lsSQL, loObject.getTable(), "", "") == 0){
            if (!poGRider.getErrMsg().isEmpty()){
                setErrMsg(poGRider.getErrMsg());
            } else setErrMsg("No record deleted.");  
        } else lbResult = true;
        
        if (!pbWithParent){
            if (getErrMsg().isEmpty()){
                poGRider.commitTrans();
            } else poGRider.rollbackTrans();
        }
        return lbResult;
    }

    @Override
    public String getMessage() {
        return psWarnMsg;
    }

    @Override
    public void setMessage(String fsMessage) {
        this.psWarnMsg = fsMessage;
    }

    @Override
    public String getErrMsg() {
        return psErrMsgx;
    }

    @Override
    public void setErrMsg(String fsErrMsg) {
        this.psErrMsgx = fsErrMsg;
    }

    @Override
    public void setBranch(String foBranchCD) {
        this.psBranchCd = foBranchCD;
    }

    @Override
    public void setWithParent(boolean fbWithParent) {
        this.pbWithParent = fbWithParent;
    }

    @Override
    public String getSQ_Master() {
        return "SELECT" +
                    "  sTransNox" +
                    ", sBranchCd" +
                    ", dTransact" +
                    ", sCompnyID" +
                    ", sSupplier" +
                    ", sReferNox" +
                    ", dRefernce" +
                    ", sTermCode" +
                    ", nTranTotl" +
                    ", nVATRatex" +
                    ", nTWithHld" +
                    ", nDiscount" +
                    ", nAddDiscx" +
                    ", nAmtPaidx" +
                    ", nFreightx" +
                    ", sRemarksx" +
                    ", sSourceNo" +
                    ", sSourceCd" +
                    ", nEntryNox" +
                    ", sInvTypCd" +
                    ", cTranStat" +
                    ", sPrepared" +
                    ", dPrepared" +
                    ", sApproved" +
                    ", dApproved" +
                    ", sAprvCode" +
                    ", sPostedxx" +
                    ", dPostedxx" +
                    ", sDeptIDxx" +
                    ", cDivision" +
                    ", sModified" +
                    ", dModified" +
                " FROM " + pxeMasTable;
    }
   
    public int ItemCount(){
        return poDetail.size();
    }
    
    public boolean addDetail() {
        if (poDetail.isEmpty()){
            poDetail.add(new UnitPOReceivingDetail());
            paDetailOthers.add(new UnitPOReceivingDetailOthers());
        }else{
            if (System.getProperty("store.inventory.strict.type").equals("1")){
                if (!poDetail.get(ItemCount()-1).getStockID().equals("") &&
                    poDetail.get(ItemCount() -1).getQuantity().doubleValue()!= 0.00){
                    poDetail.add(new UnitPOReceivingDetail());
                    paDetailOthers.add(new UnitPOReceivingDetailOthers());
                } else return false;
            } else {
                if (!poDetail.get(ItemCount()-1).getStockID().equals("") &&
                    poDetail.get(ItemCount() -1).getQuantity().doubleValue() != 0.00){
                    poDetail.add(new UnitPOReceivingDetail());
                    paDetailOthers.add(new UnitPOReceivingDetailOthers());
                } else return false;
            }   
        }
        return true;
    }

    public boolean deleteDetail(int fnEntryNox) {       
        poDetail.remove(fnEntryNox);
        paDetailOthers.remove(fnEntryNox);
        
        if (poDetail.isEmpty()) return addDetail();
        
        return true;
    }
    
    public void setDetail(int fnRow, int fnCol, Object foData){
        switch (fnCol){
            case 8: //nUnitPrce
            case 9: //nFreightx
                if (foData instanceof Number){
                    poDetail.get(fnRow).setValue(fnCol, foData);
                }else poDetail.get(fnRow).setValue(fnCol, 0);
                break;
            case 7: //nQuantity
                if (foData instanceof Number){
                    poDetail.get(fnRow).setValue(fnCol, foData);
                }else poDetail.get(fnRow).setValue(fnCol, 0);
                break;
            case 10: //dExpiryDt
                if (foData instanceof Date){
                    poDetail.get(fnRow).setValue(fnCol, foData);
                }else poDetail.get(fnRow).setValue(fnCol, null);
                break;
            case 100: //xBarCodex
            case 101: //xDescript
                if (System.getProperty("store.inventory.strict.type").equals("0")){
                    paDetailOthers.get(fnRow).setValue(fnCol, foData);
                }
                break;
             case 102: //sMeasurNm
                paDetailOthers.get(fnRow).setValue("sMeasurNm", foData);
                break;
            default:
                poDetail.get(fnRow).setValue(fnCol, foData);
        }
    }
    public void setDetail(int fnRow, String fsCol, Object foData){
        switch(fsCol){
            case "nUnitPrce":
            case "nFreightx":
                if (foData instanceof Number){
                    poDetail.get(fnRow).setValue(fsCol, foData);
                }else poDetail.get(fnRow).setValue(fsCol, 0);
                break;
            case "nQuantity":
                if (foData instanceof Number){
                    poDetail.get(fnRow).setValue(fsCol, foData);
                }else poDetail.get(fnRow).setValue(fsCol, 0);
                break;
            case "dExpiryDt":
                if (foData instanceof Date){
                    poDetail.get(fnRow).setValue(fsCol, foData);
                }else poDetail.get(fnRow).setValue(fsCol, null);
                break;
            case "xBarCodex":
                setDetail(fnRow, 100, foData);
                break;
            case "xDescript":
                setDetail(fnRow, 101, foData);
                break;
            case "sMeasurNm":
                setDetail(fnRow, 102, foData);
                break;
            default:
                poDetail.get(fnRow).setValue(fsCol, foData);
        }
    }
    
    public Object getDetail(int fnRow, int fnCol){
        switch(fnCol){
            case 100:
                return paDetailOthers.get(fnRow).getValue("xBarCodex");
            case 101:
            case 102:
                return paDetailOthers.get(fnRow).getValue("sMeasurNm");
            default:
                return poDetail.get(fnRow).getValue(fnCol);
        }
    }
    
    public Object getDetail(int fnRow, String fsCol){
        switch(fsCol){
            case "xBarCodex":
                return getDetail(fnRow, 100);
            case "xDescript":
                return getDetail(fnRow, 101);
            case "sMeasurNm":
                return getDetail(fnRow, 102);
            default:
                return poDetail.get(fnRow).getValue(fsCol);
        }       
    }
    
    private boolean saveDetail(UnitPOReceivingMaster foData, boolean fbNewRecord) throws SQLException{
        UnitPOReceivingDetail loDetail;
        UnitPOReceivingDetail loOldDet;
        
        String lsSQL;
        
        int lnCtr;
        int lnRow = 0;
        
        for (lnCtr = 0; lnCtr <= ItemCount() - 1; lnCtr++){      
            if (!poDetail.get(lnCtr).getStockID().equals("")){
                if (Double.parseDouble(poDetail.get(lnCtr).getQuantity().toString()) <= 0.00){
                    setMessage("Unable to save zero quantity detail.");
                    return false;
                }
                
                poDetail.get(lnCtr).setTransNox(foData.getTransNox());
                poDetail.get(lnCtr).setEntryNox(lnCtr + 1);
                poDetail.get(lnCtr).setDateModified(poGRider.getServerDate());
                
                if (!poDetail.get(lnCtr).getStockID().equals("")){
                    if (fbNewRecord){
                        //Generate the SQL Statement
                        lsSQL = MiscUtil.makeSQL((GEntity) poDetail.get(lnCtr),
                                                    "sBrandNme");
                    }else{
                        //Load previous transaction
                        loOldDet = loadTransDetail(foData.getTransNox(), lnCtr + 1);

                        //Generate the Update Statement
                        lsSQL = MiscUtil.makeSQL((GEntity) poDetail.get(lnCtr), (GEntity) loOldDet, 
                                                    "sTransNox = " + SQLUtil.toSQL(poDetail.get(lnCtr).getTransNox()) + 
                                                        " AND nEntryNox = " + poDetail.get(lnCtr).getEntryNox(),
                                                    "sBrandNme");
                    }

                    if (!lsSQL.equals("")){
                        if(poGRider.executeQuery(lsSQL, pxeDetTable, "", "") == 0){
                            if(!poGRider.getErrMsg().isEmpty()){ 
                                setErrMsg(poGRider.getErrMsg());
                                return false;
                            }

                        }else {
                            lnRow += 1;
                        }
                    }
                }
            }
        }    
        
        if (lnRow == 0) {
            setMessage("No record to update.");
            return false;
        }
        
        //check if the new detail is less than the original detail count
        lnRow = loadTransDetail(foData.getTransNox()).size();
        if (lnCtr < lnRow -1){
            for (lnCtr = lnCtr + 1; lnCtr <= lnRow; lnCtr++){
                lsSQL = "DELETE FROM " + pxeDetTable +  
                        " WHERE sTransNox = " + SQLUtil.toSQL(foData.getTransNox()) + 
                            " AND nEntryNox = " + lnCtr;
                
                if(poGRider.executeQuery(lsSQL, pxeDetTable, "", "") == 0){
                    if(!poGRider.getErrMsg().isEmpty()) setErrMsg(poGRider.getErrMsg());
                }else {
                    setMessage("No record updated");
                    return false;
                }
            }
        }
        
        return true;
    }
                         
    private double computeUnitPrice(UnitPOReceivingDetail loDetail) {
        try{
            UnitPOReceivingDetail loOldDet = loadInvDetail(loDetail.getStockID());
            double lnQty = Double.parseDouble(loDetail.getQuantity().toString());
            double lnQty1 = Double.parseDouble(loOldDet.getQuantity().toString());
            double lnValue = Double.parseDouble(loDetail.getUnitPrice().toString());
            double lnValue1 =  Double.parseDouble(loOldDet.getUnitPrice().toString());
            System.out.println("new qty = " + lnQty);
            System.out.println("old qty= " + lnQty1);
            System.out.println("new price = " + lnValue);
            System.out.println("old price= " + lnValue1);
            System.out.println("new = " + (lnValue * lnQty));
            System.out.println("old = " + (lnValue1 * lnQty1));
            if(Double.parseDouble(loOldDet.getUnitPrice().toString())> 0){
                if(!loDetail.getUnitPrice().equals(loOldDet.getUnitPrice())){
                    double avgCost = ((lnValue * lnQty) + (lnValue1 * lnQty1)) / (lnQty + lnQty1);
//                    poDetail.get(lnRow).setUnitPrice(avgCost);
                    
                    return avgCost;
                }
            }
        }catch(SQLException e){
            
        }
        
        return 0.00;
    }
    private UnitPOReceivingDetail loadTransDetail(String fsTransNox, int fnEntryNox) throws SQLException{
        UnitPOReceivingDetail loObj = null;
        ResultSet loRS = poGRider.executeQuery(
                            MiscUtil.addCondition(getSQ_Detail(), 
                                                    "sTransNox = " + SQLUtil.toSQL(fsTransNox)) + 
                                                    " AND nEntryNox = " + fnEntryNox);
        
        if (!loRS.next()){
            setMessage("No Record Found");
        }else{
            //load each column to the entity
            loObj = new UnitPOReceivingDetail();
            for(int lnCol=1; lnCol<=loRS.getMetaData().getColumnCount(); lnCol++){
                if(lnCol<=11){
                    
                    loObj.setValue(lnCol, loRS.getObject(lnCol));
                    
                }else if(lnCol == 19){
                        loObj.setValue(12, loRS.getObject(lnCol));
                }
            }
        }      
        return loObj;
    }
    
    private UnitPOReceivingDetail loadInvDetail(String fsTransNox) throws SQLException{
         UnitPOReceivingDetail loObj = null;
         System.out.println(MiscUtil.addCondition(getSQ_Stock(), 
                                                    "a.sStockIDx = " + SQLUtil.toSQL(fsTransNox)));
        ResultSet loRS = poGRider.executeQuery(
                            MiscUtil.addCondition(getSQ_Stock(), 
                                                    "a.sStockIDx = " + SQLUtil.toSQL(fsTransNox)));
        
        if (!loRS.next()){
            setMessage("No Record Found");
        }else{
            //load each column to the entity
            loObj = new UnitPOReceivingDetail();
            for(int lnCol=1; lnCol<=loRS.getMetaData().getColumnCount(); lnCol++){
                if(lnCol<=9){
                    if(lnCol == 2){
                        loObj.setValue(lnCol, 1);
                    }else loObj.setValue(lnCol, loRS.getObject(lnCol));
                    
                }else if(lnCol >= 10 && lnCol <=11){
                        loObj.setValue(lnCol, poGRider.getServerDate());
                }else if(lnCol == 19){
                        loObj.setValue(12, loRS.getObject(lnCol));
                }
            }
        }      
        return loObj;
    }
    
    private ArrayList<UnitPOReceivingDetail> loadTransDetail(String fsTransNox) throws SQLException{
        UnitPOReceivingDetail loOcc = null;
        UnitPOReceivingDetailOthers loOth = null;
        Connection loConn = null;
        loConn = setConnection();
        
        ArrayList<UnitPOReceivingDetail> loDetail = new ArrayList<>();
        paDetailOthers = new ArrayList<>(); //reset detail other
        System.out.println("getSQ_Detail() = " + MiscUtil.addCondition(getSQ_Detail(), 
                                                    "sTransNox = " + SQLUtil.toSQL(fsTransNox)));
        
        ResultSet loRS = poGRider.executeQuery(
                            MiscUtil.addCondition(getSQ_Detail(), 
                                                    "sTransNox = " + SQLUtil.toSQL(fsTransNox)));
        
        for (int lnCtr = 1; lnCtr <= MiscUtil.RecordCount(loRS); lnCtr ++){
            loRS.absolute(lnCtr);
            int lnCol1 = 1;
            
            loOcc = new UnitPOReceivingDetail();
            
            loOth = new UnitPOReceivingDetailOthers();
            for(int lnCol=1; lnCol<=loRS.getMetaData().getColumnCount(); lnCol++){
//                loOcc.setValue(lnCol, loRS.getObject(lnCol));
                System.out.println("lnCol = " + lnCol + " : " + loRS.getObject(lnCol));
                if(lnCol<=11){
                    if(lnCol == 4){
                        loOth.setValue(1, loRS.getObject(lnCol));
                    }
                    loOcc.setValue(lnCol, loRS.getObject(lnCol));
                    if(lnCol == 8){
                       
                    }
                }else if(lnCol >=12 && lnCol<19){
                        loOth.setValue(lnCol1, loRS.getObject(lnCol));
                }else if(lnCol == 19){
                        loOcc.setValue(12, loRS.getObject(lnCol));
                }
                lnCol1++;
            }
            
            
//            loOcc.setValue("sTransNox", loRS.getObject("sTransNox"));        
//            loOcc.setValue("nEntryNox", loRS.getObject("nEntryNox"));
//            loOcc.setValue("sOrderNox", loRS.getObject("sOrderNox"));
//            loOcc.setValue("sStockIDx", loRS.getObject("sStockIDx"));
//            loOcc.setValue("sReplacID", loRS.getObject("sReplacID"));
//            loOcc.setValue("cUnitType", loRS.getObject("cUnitType"));
//            loOcc.setValue("nQuantity", loRS.getObject("nQuantity"));
//            loOcc.setValue("nUnitPrce", loRS.getObject("nUnitPrce"));
//            loOcc.setValue("nFreightx", loRS.getObject("nFreightx"));
//            loOcc.setValue("dExpiryDt", loRS.getObject("dExpiryDt"));
//            loOcc.setValue("dModified", loRS.getObject("dModified"));
//            loOcc.setValue("sBrandNme", loRS.getObject("sBrandNme"));
            
            loDetail.add(loOcc);
            //load other info
//            loOth = new UnitPOReceivingDetailOthers();
//            loOth.setValue("sStockIDx", loRS.getObject("sStockIDx"));
//            loOth.setValue("nQtyOnHnd", loRS.getObject("nQtyOnHnd"));
//            loOth.setValue("xQtyOnHnd", loRS.getObject("xQtyOnHnd"));
//            loOth.setValue("nResvOrdr", loRS.getObject("nResvOrdr"));
//            loOth.setValue("nBackOrdr", loRS.getObject("nBackOrdr"));
//            loOth.setValue("nReorderx", 0);
//            loOth.setValue("nLedgerNo", loRS.getInt("nLedgerNo"));
//            loOth.setValue("sMeasurNm", loRS.getObject("sMeasurNm"));
            paDetailOthers.add(loOth);
        }
        
        return loDetail;
    }
    
    private String getSQ_Detail(){
        return "SELECT" +
                    "  a.sTransNox" +
                    ", a.nEntryNox" +
                    ", a.sOrderNox" +
                    ", a.sStockIDx" +
                    ", a.sReplacID" +
                    ", a.cUnitType" +
                    ", a.nQuantity" +
                    ", a.nUnitPrce" +
                    ", a.nFreightx" +
                    ", a.dExpiryDt" +
                    ", a.dModified" +
                    ", IFNULL(b.nQtyOnHnd, 0) nQtyOnHnd" + 
                    ", IFNULL(b.nQtyOnHnd, 0) + a.nQuantity xQtyOnHnd" + 
                    ", IFNULL(b.nResvOrdr, 0) nResvOrdr" +
                    ", IFNULL(b.nBackOrdr, 0) nBackOrdr" +
                    ", IFNULL(b.nFloatQty, 0) nFloatQty" +
                    ", IFNULL(b.nLedgerNo, 0) nLedgerNo" +
                    ", IFNULL(e.sMeasurNm, '') sMeasurNm" +
                    ", IFNULL(f.sDescript, '') sBrandNme" +
                " FROM " + pxeDetTable + " a" +
                    " LEFT JOIN Inventory d" + 
                        " ON a.sReplacID = d.sStockIDx" + 
                    " LEFT JOIN Inv_Master b" +
                        " ON a.sStockIDx = b.sStockIDx" + 
                            " AND b.sBranchCD = " + SQLUtil.toSQL(psBranchCd) +
                    " LEFT JOIN Inventory c" + 
                        " ON b.sStockIDx = c.sStockIDx" +  
                    " LEFT JOIN Brand f" + 
                        " ON c.sBrandCde = f.sBrandCde" +  
                    " LEFT JOIN Measure e" +
                        " ON c.sMeasurID = e.sMeasurID" +
                " ORDER BY a.nEntryNox";
    }
     private String getSQ_Stock(){
        return "SELECT" +
                    "  '' sTransNox" +
                    ", 0 nEntryNox" +
                    ", '' sOrderNox" +
                    ", a.sStockIDx" +
                    ", '' sReplacID" +
                    ", '' cUnitType" +
                    ", IFNULL(b.nQtyOnHnd, 0)  nQuantity" +
                    ", a.nUnitPrce" +
                    ", 0 nFreightx" +
                    ", '' dExpiryDt" +
                    ", '' dModified" +
                    ", IFNULL(b.nQtyOnHnd, 0) nQtyOnHnd" + 
                    ", IFNULL(b.nQtyOnHnd, 0) xQtyOnHnd" + 
                    ", IFNULL(b.nResvOrdr, 0) nResvOrdr" +
                    ", IFNULL(b.nBackOrdr, 0) nBackOrdr" +
                    ", IFNULL(b.nFloatQty, 0) nFloatQty" +
                    ", IFNULL(b.nLedgerNo, 0) nLedgerNo" +
                    ", IFNULL(d.sMeasurNm, '') sMeasurNm" +
                    ", IFNULL(c.sDescript, '') sBrandNme" +
                " FROM  Inventory a" + 
                    " LEFT JOIN Inv_Master b" +
                        " ON a.sStockIDx = b.sStockIDx" + 
                            " AND b.sBranchCD = " + SQLUtil.toSQL(psBranchCd) +
                    " LEFT JOIN Brand c" + 
                        " ON a.sBrandCde = c.sBrandCde" +  
                    " LEFT JOIN Measure d" +
                        " ON a.sMeasurID = d.sMeasurID" +
                " ORDER BY a.sStockIDx";
    }
    
    //Added methods
    private boolean saveInvUnitPrice(){
        String lsBarCode = "";
        String lsStockID = "";
        String lsSQL = "";
        Inventory loInventory;
        InventoryTrans loInvTrans = new InventoryTrans(poGRider, poGRider.getBranchCode());
        loInvTrans.InitTransaction();
        
        
        for (int lnCtr = 0; lnCtr <= poDetail.size() - 1; lnCtr ++){
            if (poDetail.get(lnCtr).getStockID().equals("")) break;
            
            
            lsStockID = poDetail.get(lnCtr).getStockID();
//            lsBarCode = paDetailOthers.get(lnCtr).getValue("xBarCodex").toString();
//            lsBarCode = getDetail(lnCtr, 100).toString();
            
            if (!lsStockID.equals("")) {
                loInventory =  GetInventory(lsStockID, true, false);
                lsBarCode = loInventory.getMaster("sBarCodex").toString();
                lsSQL = "UPDATE Inventory SET" +
                            "  nUnitPrce = " + poDetail.get(lnCtr).getUnitPrice()+
                        " WHERE sStockIDx = " + SQLUtil.toSQL(lsStockID) +
                            " AND sBarCodex = " + SQLUtil.toSQL(lsBarCode) ;
                
                if (poGRider.executeQuery(lsSQL, "Inventory", psBranchCd, "") <= 0){
                    setMessage("Unable to update inventory unit price.");
                    return false;
                }
            }  
        }
        

        return saveInvAvgCost();
    }
    
    public Inventory GetInventory(String fsValue, boolean fbByCode, boolean fbSearch){        
        Inventory instance = new Inventory(poGRider, psBranchCd, fbSearch);
        instance.BrowseRecord(fsValue, fbByCode, false);
        return instance;
    }
    //Added methods
    private boolean saveInvAvgCost(){
        String lsStockID = "";
        String lsSQL = "";
        
        InventoryTrans loInvTrans = new InventoryTrans(poGRider, poGRider.getBranchCode());
        loInvTrans.InitTransaction();
        
        
        for (int lnCtr = 0; lnCtr <= poDetail.size() - 1; lnCtr ++){
            if (poDetail.get(lnCtr).getStockID().equals("")) break;
            
            lsStockID = poDetail.get(lnCtr).getStockID();
            if (!lsStockID.equals("")) {
                lsSQL = "UPDATE Inv_Master SET" +
                            "  nAvgCostx = + " + computeUnitPrice(poDetail.get(lnCtr)) +
                        " WHERE sStockIDx = " + SQLUtil.toSQL(lsStockID)+
                            " AND sBranchCd = " + SQLUtil.toSQL(psBranchCd) ;
                
                if (poGRider.executeQuery(lsSQL, "Inv_Master", psBranchCd, "") <= 0){
                    setMessage("Unable to update inventory average cost.");
                    return false;
                }
            }  
        }
        

        return true;
    }
    
    private boolean saveInvTrans(String fsTransNox, String fsSupplier, Date fdTransact){
        String lsOrderNo = "";
        String lsSQL = "";
        
        InventoryTrans loInvTrans = new InventoryTrans(poGRider, poGRider.getBranchCode());
        loInvTrans.InitTransaction();
        
        
        for (int lnCtr = 0; lnCtr <= poDetail.size() - 1; lnCtr ++){
            if (poDetail.get(lnCtr).getStockID().equals("")) break;
            loInvTrans.setDetail(lnCtr, "sStockIDx", poDetail.get(lnCtr).getStockID());
            loInvTrans.setDetail(lnCtr, "sReplacID", poDetail.get(lnCtr).getReplaceID());
            loInvTrans.setDetail(lnCtr, "nQuantity", poDetail.get(lnCtr).getQuantity());
            loInvTrans.setDetail(lnCtr, "nPurchase", poDetail.get(lnCtr).getUnitPrice());
            loInvTrans.setDetail(lnCtr, "nQtyOnHnd", paDetailOthers.get(lnCtr).getValue("nQtyOnHnd"));
            loInvTrans.setDetail(lnCtr, "nResvOrdr", paDetailOthers.get(lnCtr).getValue("nResvOrdr"));
            loInvTrans.setDetail(lnCtr, "nBackOrdr", paDetailOthers.get(lnCtr).getValue("nBackOrdr"));
            loInvTrans.setDetail(lnCtr, "nLedgerNo", paDetailOthers.get(lnCtr).getValue("nLedgerNo"));
            loInvTrans.setDetail(lnCtr, "dExpiryDt", poDetail.get(lnCtr).getDateExpiry());
            
            lsOrderNo = poDetail.get(lnCtr).getOrderNox();
            
            if (!lsOrderNo.equals("")) {
                lsSQL = "UPDATE PO_Detail SET" +
                            "  nReceived = nReceived + " + poDetail.get(lnCtr).getQuantity() +
                        " WHERE sTransNox = " + SQLUtil.toSQL(lsOrderNo) +
                            " AND nEntryNox = " + poDetail.get(lnCtr).getEntryNox();
                
                if (poGRider.executeQuery(lsSQL, "PO_Detail", psBranchCd, "") <= 0){
                    setMessage("Unable to update order reference.");
                    return false;
                }
            }  
        }
        
        if (!loInvTrans.POReceiving(fsTransNox, fdTransact, fsSupplier, EditMode.ADDNEW)){
            setMessage(loInvTrans.getMessage());
            setErrMsg(loInvTrans.getErrMsg());
            return false;
        }

        return saveInvExpiration(fdTransact);
    }
    
    private boolean unsaveInvTrans(String fsTransNox, String fsSupplier){
        String lsOrderNo = "";
        String lsSQL = "";
        
        InventoryTrans loInvTrans = new InventoryTrans(poGRider, poGRider.getBranchCode());
        loInvTrans.InitTransaction();
        
        for (int lnCtr = 0; lnCtr <= poDetail.size() - 1; lnCtr ++){
            loInvTrans.setDetail(lnCtr, "sStockIDx", poDetail.get(lnCtr).getStockID());
            loInvTrans.setDetail(lnCtr, "nQtyOnHnd", paDetailOthers.get(lnCtr).getValue("nQtyOnHnd"));
            loInvTrans.setDetail(lnCtr, "nResvOrdr", paDetailOthers.get(lnCtr).getValue("nResvOrdr"));
            loInvTrans.setDetail(lnCtr, "nBackOrdr", paDetailOthers.get(lnCtr).getValue("nBackOrdr"));
            loInvTrans.setDetail(lnCtr, "nLedgerNo", paDetailOthers.get(lnCtr).getValue("nLedgerNo"));
        
            lsOrderNo = poDetail.get(lnCtr).getOrderNox();
            
            if (!lsOrderNo.equals("")) {
                lsSQL = "UPDATE PO_Detail SET" +
                            "  nReceived = nReceived - " + poDetail.get(lnCtr).getQuantity() +
                        " WHERE sTransNox = " + SQLUtil.toSQL(lsOrderNo) +
                            " AND nEntryNox = " + poDetail.get(lnCtr).getEntryNox();
                
                if (poGRider.executeQuery(lsSQL, "PO_Detail", psBranchCd, "") <= 0){
                    setMessage("Unable to update order reference.");
                    return false;
                }
            }  
        }
        
        if (!loInvTrans.POReceiving(fsTransNox, poGRider.getServerDate(), fsSupplier, EditMode.DELETE)){
            setMessage(loInvTrans.getMessage());
            setErrMsg(loInvTrans.getErrMsg());
            return false;
        }

        return true;
    }
    
    private boolean saveInvExpiration(Date fdTransact){
        InvExpiration loInvTrans = new InvExpiration(poGRider, poGRider.getBranchCode());
        loInvTrans.InitTransaction();
        
        for (int lnCtr = 0; lnCtr <= poDetail.size() - 1; lnCtr ++){
            if (poDetail.get(lnCtr).getStockID().equals("")) break;
            loInvTrans.setDetail(lnCtr, "sStockIDx", poDetail.get(lnCtr).getStockID());
            loInvTrans.setDetail(lnCtr, "dExpiryDt", poDetail.get(lnCtr).getDateExpiry());
            loInvTrans.setDetail(lnCtr, "nQtyInxxx", poDetail.get(lnCtr).getQuantity());
        }
        
        if (!loInvTrans.POReceiving(fdTransact, EditMode.ADDNEW)){
            setMessage(loInvTrans.getMessage());
            setErrMsg(loInvTrans.getErrMsg());
            return false;
        }

        return saveInvUnitPrice();
    }
    
    public void setGRider(GRider foGRider){
        this.poGRider = foGRider;
        this.psUserIDxx = foGRider.getUserID();
        
        if (psBranchCd.isEmpty()) psBranchCd = foGRider.getBranchCode();
        
        poDetail = new ArrayList<>();
    }
    
    
    public void setUserID(String fsUserID){
        this.psUserIDxx  = fsUserID;
    }
    
    private Connection setConnection(){
        Connection foConn;
        
        if (pbWithParent){
            foConn = (Connection) poGRider.getConnection();
            if (foConn == null) foConn = (Connection) poGRider.doConnect();
        }else foConn = (Connection) poGRider.doConnect();
        
        return foConn;
    }
    
    //Member Variables
    private GRider poGRider = null;
    private String psUserIDxx = "";
    private String psBranchCd = "";
    private String psWarnMsg = "";
    private String psErrMsgx = "";
    private boolean pbWithParent = false;
    private final GCrypt poCrypt = new GCrypt();
    
    private ArrayList<UnitPOReceivingDetail> poDetail;
    private ArrayList<UnitPOReceivingDetailOthers> paDetailOthers;
    private final String pxeMasTable = "PO_Receiving_Master";
    private final String pxeDetTable = "PO_Receiving_Detail";
    private final String pxeModuleName = POReceiving.class.getSimpleName();
    
    @Override
    public boolean closeTransaction(String string) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}