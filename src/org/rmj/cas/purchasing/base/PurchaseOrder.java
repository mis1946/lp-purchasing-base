package org.rmj.cas.purchasing.base;

import com.mysql.jdbc.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.rmj.appdriver.GCrypt;
import org.rmj.appdriver.GRider;
import org.rmj.appdriver.iface.GEntity;
import org.rmj.appdriver.iface.GTransaction;
import org.rmj.appdriver.MiscUtil;
import org.rmj.appdriver.SQLUtil;
import org.rmj.appdriver.agentfx.CommonUtils;
import org.rmj.appdriver.constants.TransactionStatus;
import org.rmj.cas.inventory.base.Inventory;
import org.rmj.cas.purchasing.pojo.UnitPODetail;
import org.rmj.cas.purchasing.pojo.UnitPOMaster;
import org.rmj.cas.purchasing.pojo.UnitPOReceivingDetailOthers;

public class PurchaseOrder implements GTransaction{
    @Override
    public UnitPOMaster newTransaction() {
        UnitPOMaster loObj = new UnitPOMaster();
        Connection loConn = null;
        loConn = setConnection();
        
        loObj.setTransNox(MiscUtil.getNextCode(loObj.getTable(), "sTransNox", true, loConn, psBranchCd));
        
        //init detail
        poDetail = new ArrayList<>();
        paDetailOthers = new ArrayList<>();
        
        return loObj;
    }

    @Override
    public UnitPOMaster loadTransaction(String fsTransNox) {
        UnitPOMaster loObject = new UnitPOMaster();
        
        Connection loConn = null;
        loConn = setConnection();   
        
        String lsSQL = MiscUtil.addCondition(getSQ_Master(), "sTransNox = " + SQLUtil.toSQL(fsTransNox));
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
    public UnitPOMaster saveUpdate(Object foEntity, String fsTransNox) {
        String lsSQL = "";
        
        UnitPOMaster loOldEnt = null;
        UnitPOMaster loNewEnt = null;
        UnitPOMaster loResult = null;
        
        // Check for the value of foEntity
        if (!(foEntity instanceof UnitPOMaster)) {
            setErrMsg("Invalid Entity Passed as Parameter");
            return loResult;
        }
        
        // Typecast the Entity to this object
        loNewEnt = (UnitPOMaster) foEntity;
        
        
        // Test if entry is ok
        if (loNewEnt.getBranchCd()== null || loNewEnt.getBranchCd().isEmpty()){
            setMessage("No branch detected.");
            return loResult;
        }
        
        if (loNewEnt.getDateTransact()== null){
            setMessage("No transact date detected.");
            return loResult;
        }
        
        if (loNewEnt.getCompanyID()== null || loNewEnt.getCompanyID().isEmpty()){
            setMessage("No company detected.");
            return loResult;
        }
        
        if (loNewEnt.getDestinat()== null || loNewEnt.getDestinat().isEmpty()){
            setMessage("No destination detected.");
            return loResult;
        }
        
        if (loNewEnt.getSupplier()== null || loNewEnt.getSupplier().isEmpty()){
            setMessage("No supplier detected.");
            return loResult;
        }
        
        /*
        if (loNewEnt.getSourceNo()== null || loNewEnt.getSourceNo().isEmpty()){
            setMessage("Invalid source number detected.");
            return loResult;
        }
        
        if (loNewEnt.getSourceCd()== null || loNewEnt.getSourceCd().isEmpty()){
            setMessage("Invalid source code detected.");
            return loResult;
        }*/
        
//        if (loNewEnt.getInvTypeCd()== null || loNewEnt.getInvTypeCd().isEmpty()){
//            setMessage("Invalid inventory type detected.");
//            return loResult;
//        }
               
        if (!pbWithParent) poGRider.beginTrans();
        
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
                if (!saveDetail(loNewEnt, true)){
                    poGRider.rollbackTrans();
                    return loResult;
                }
                
                loNewEnt.setEntryNox(ItemCount());
                
                if (!pbWithParent) MiscUtil.close(loConn);
                
                //Generate the SQL Statement
                lsSQL = MiscUtil.makeSQL((GEntity) loNewEnt);
            } catch (SQLException ex) {
                Logger.getLogger(PurchaseOrder.class.getName()).log(Level.SEVERE, null, ex);
            }
        }else{
            try {
                //Load previous transaction
                loOldEnt = loadTransaction(fsTransNox);
                
                //save detail first
                if (!saveDetail(loNewEnt, true)) {
                    poGRider.rollbackTrans();
                    return loResult;
                }
                
                loNewEnt.setEntryNox(ItemCount());
                loNewEnt.setDateModified(poGRider.getServerDate());
                
                //Generate the Update Statement
                lsSQL = MiscUtil.makeSQL((GEntity) loNewEnt, (GEntity) loOldEnt, "sTransNox = " + SQLUtil.toSQL(loNewEnt.getTransNox()));
            } catch (SQLException ex) {
                Logger.getLogger(PurchaseOrder.class.getName()).log(Level.SEVERE, null, ex);
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
            else
            setMessage("No record updated");
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
        UnitPOMaster loObject = loadTransaction(fsTransNox);
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

    @Override
    public boolean postTransaction(String fsTransNox) {
        UnitPOMaster loObject = loadTransaction(fsTransNox);
        boolean lbResult = false;
        
        if (loObject == null){
            setMessage("No record found...");
            return lbResult;
        }
        
        if (!loObject.getTranStat().equalsIgnoreCase(TransactionStatus.STATE_CLOSED)){
            setMessage("Unable to post un-printed transaction.");
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
        } else lbResult = true;
        
        if (!pbWithParent){
            if (getErrMsg().isEmpty()){
                poGRider.commitTrans();
            } else poGRider.rollbackTrans();
        }
        return lbResult;
    }

    @Override
    public boolean voidTransaction(String fsTransNox) {
        UnitPOMaster loObject = loadTransaction(fsTransNox);
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
        UnitPOMaster loObject = loadTransaction(fsTransNox);
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
                    ", sDestinat" +
                    ", sSupplier" +
                    ", sReferNox" +
                    ", sTermCode" +
                    ", nTranTotl" +
                    ", sRemarksx" +
                    ", sSourceNo" +
                    ", sSourceCd" +
                    ", cEmailSnt" +
                    ", nEmailSnt" +
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
                    ", sModified" +
                    ", dModified" +
                " FROM " + pxeMasTable;
    }
    
    //Added detail methods   
    public boolean closeTransaction(String fsTransNox, String fsUserIDxx, String fsAprvCode) {
        UnitPOMaster loObject = loadTransaction(fsTransNox);
        boolean lbResult = false;
        
        if (loObject == null){
            setMessage("No record found...");
            return lbResult;
        }
        
        if (fsAprvCode == null || fsAprvCode.isEmpty()){
            setMessage("Invalid/No approval code detected.");
            return lbResult;
        }
        
        if (!loObject.getTranStat().equalsIgnoreCase(TransactionStatus.STATE_OPEN)){
            setMessage("Unable to close closed/cancelled/posted/voided transaction.");
            return lbResult;
        }
        
        String lsSQL = "UPDATE " + loObject.getTable() + 
                        " SET  cTranStat = " + SQLUtil.toSQL(TransactionStatus.STATE_CLOSED) + 
                            ", sApproved = " + SQLUtil.toSQL(fsUserIDxx) +
                            ", dApproved = " + SQLUtil.toSQL(poGRider.getServerDate()) + 
                            ", sAprvCode = " + SQLUtil.toSQL(fsAprvCode) +
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
    
    public int ItemCount(){
        return poDetail.size();
    }
    
    public boolean addDetail() {
        //UnitPODetail loDetail = new UnitPODetail();
        //poDetail.add(loDetail);
        
        if (poDetail.isEmpty()){
            poDetail.add(new UnitPODetail());
            paDetailOthers.add(new UnitPOReceivingDetailOthers());
        }else{
            if (!poDetail.get(ItemCount()-1).getStockID().equals("") &&
                        poDetail.get(ItemCount() -1).getQuantity().doubleValue()!= 0){
                poDetail.add(new UnitPODetail());
                paDetailOthers.add(new UnitPOReceivingDetailOthers());
            }
            
//            if ((!poDetail.get(ItemCount()-1).getStockID().equals("") ||
//                    !paDetailOthers.get(ItemCount()-1).getValue(101).equals("")) &&
//                        poDetail.get(ItemCount() -1).getQuantity().doubleValue()!= 0){
//                poDetail.add(new UnitPODetail());
//                paDetailOthers.add(new UnitPOReceivingDetailOthers());
//            }
        }
        return true;
    }

    public boolean deleteDetail(int fnEntryNox) {
        //poDetail.remove(fnEntryNox);
        //if (poDetail.isEmpty()) return addDetail();
        
        poDetail.remove(fnEntryNox);
        paDetailOthers.remove(fnEntryNox);
        
        if (poDetail.isEmpty()) return addDetail();
        
        return true;
    }
    
    public void setDetail(int fnRow, int fnCol, Object foData){
        switch (fnCol){
            case 5: //nUnitPrce
                if (foData instanceof Number){
                    poDetail.get(fnRow).setValue(fnCol, foData);
                }else poDetail.get(fnRow).setValue(fnCol, 0);
                break;
            case 2: //nEntryNox
            case 4: //nQuantity
                if (foData instanceof Number){
                    poDetail.get(fnRow).setValue(fnCol, foData);
                }else poDetail.get(fnRow).setValue(fnCol, 0);
                break;
            case 6: //nReceived
            case 7: //nCancelld
                if (foData instanceof Number){
                    poDetail.get(fnRow).setValue(fnCol, foData);
                }else poDetail.get(fnRow).setValue(fnCol, 0);
                break;
            case 100: //xBarCodex
            case 101: //xDescript
                paDetailOthers.get(fnRow).setValue(fnCol, foData);
                break;
            default:
                
                poDetail.get(fnRow).setValue(fnCol, foData);
        }
    }
    public void setDetail(int fnRow, String fsCol, Object foData){
        switch(fsCol){
            case "nUnitPrce":
                if (foData instanceof Number){
                    poDetail.get(fnRow).setValue(fsCol, foData);
                }else poDetail.get(fnRow).setValue(fsCol, 0);
                break;
            case "nEntryNox":
            case "nQuantity":
            case "nReceived":
            case "nCancelld":
                if (foData instanceof Number){
                    poDetail.get(fnRow).setValue(fsCol, foData);
                }else poDetail.get(fnRow).setValue(fsCol, 0);
                break;
            case "xBarCodex":
                setDetail(fnRow, 100, foData);
                break;
            case "xDescript":
                setDetail(fnRow, 101, foData);
                break;
            default:
                poDetail.get(fnRow).setValue(fsCol, foData);
        }
    }
    
    public Object getDetail(int fnRow, int fnCol){
        switch(fnCol){
            case 100:
            case 101:
                return paDetailOthers.get(fnRow).getValue(fnCol);
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
            default:
                return poDetail.get(fnRow).getValue(fsCol);
        }  
    }
    
    private boolean saveDetail(UnitPOMaster foData, boolean fbNewRecord) throws SQLException{
        if (ItemCount() <= 0){
            setMessage("No transaction detail detected.");
            return false;
        }
        
        UnitPODetail loDetail;
        UnitPODetail loOldDet;
        int lnCtr;
        String lsSQL;
        
        for (lnCtr = 0; lnCtr <= ItemCount() -1; lnCtr++){
            if (lnCtr == 0){
                if (poDetail.get(lnCtr).getStockID() == null || poDetail.get(lnCtr).getStockID().isEmpty()){
                    setMessage("Invalid stock id detected.");
                    return false;
                }
            }else {
                if (poDetail.get(lnCtr).getStockID() == null || poDetail.get(lnCtr).getStockID().isEmpty()){ 
                    poDetail.remove(lnCtr);
                    return true;
                }
            }
            if (poDetail.get(lnCtr).getStockID().equals("") &&
                !paDetailOthers.get(lnCtr).getValue(101).equals("")){
            
                //create inventory.
                Inventory loInv = new Inventory(poGRider, psBranchCd, true);
                loInv.NewRecord();
                
                if (paDetailOthers.get(lnCtr).getValue(100).equals(""))
                    loInv.setMaster("sBarCodex", CommonUtils.getNextReference(poGRider.getConnection(), "Inventory", "sBarCodex", true));
                else
                    loInv.setMaster("sBarCodex", paDetailOthers.get(lnCtr).getValue(100));
                    
                loInv.setMaster("sDescript", paDetailOthers.get(lnCtr).getValue(101));
                loInv.setMaster("sInvTypCd", foData.getInvTypeCd());
                loInv.setMaster("nUnitPrce", 0.00);
                loInv.setMaster("nSelPrice", 0.00);
                if (!loInv.SaveRecord()){
                    setMessage(loInv.getErrMsg() + "; " + loInv.getMessage());
                    return false;
                }
                
                poDetail.get(lnCtr).setStockID((String) loInv.getMaster("sStockIDx"));
            }
            
            poDetail.get(lnCtr).setTransNox(foData.getTransNox());
            poDetail.get(lnCtr).setEntryNox(lnCtr + 1);
            poDetail.get(lnCtr).setDateModified(poGRider.getServerDate());
            
            if (!poDetail.get(lnCtr).getStockID().equals("")){
                if (fbNewRecord){
                    //Generate the SQL Statement
                    lsSQL = MiscUtil.makeSQL((GEntity) poDetail.get(lnCtr),"nQtyOnHnd");
                }else{
                    //Load previous transaction
                    loOldDet = loadTransDetail(foData.getTransNox(), lnCtr + 1);

                    //Generate the Update Statement
                    lsSQL = MiscUtil.makeSQL((GEntity) poDetail.get(lnCtr), (GEntity) loOldDet, 
                                                "sTransNox = " + SQLUtil.toSQL(poDetail.get(lnCtr).getTransNox()) + 
                                                    " AND nEntryNox = " + poDetail.get(lnCtr).getEntryNox(),
                                                "nQtyOnHnd");
                }

                if (!lsSQL.equals("")){
//                        lsSQL = MiscUtil.rowset2SQL(p_oMaster, "Incentive_Master", "xBranchNm;xDeptName");
                    if(poGRider.executeQuery(lsSQL, pxeDetTable, "", "") == 0){
                        if(!poGRider.getErrMsg().isEmpty()){ 
                            setErrMsg(poGRider.getErrMsg());
                            return false;
                        }
                    }else {
                        setMessage("No record updated");
                    }
                }
            }
        }    
        
        //check if the new detail is less than the original detail count
        int lnRow = loadTransDetail(foData.getTransNox()).size();
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
    
    private UnitPODetail loadTransDetail(String fsTransNox, int fnEntryNox) throws SQLException{
        UnitPODetail loObj = null;
        ResultSet loRS = poGRider.executeQuery(
                            MiscUtil.addCondition(getSQ_Detail(), 
                                                    "sTransNox = " + SQLUtil.toSQL(fsTransNox)) + 
                                                    " AND nEntryNox = " + fnEntryNox);
        
        if (!loRS.next()){
            setMessage("No Record Found");
        }else{
            //load each column to the entity
            loObj = new UnitPODetail();
            for(int lnCol=1; lnCol<=loRS.getMetaData().getColumnCount(); lnCol++){
                loObj.setValue(lnCol, loRS.getObject(lnCol));
            }
        }  
        return loObj;
    }
    
    private ArrayList<UnitPODetail> loadTransDetail(String fsTransNox) throws SQLException{
        UnitPODetail loOcc = null;
        UnitPOReceivingDetailOthers loOth = null;
        Connection loConn = null;
        loConn = setConnection();
        
        ArrayList<UnitPODetail> loDetail = new ArrayList<>();
        paDetailOthers = new ArrayList<>(); //reset detail others
        
        ResultSet loRS = poGRider.executeQuery(
                            MiscUtil.addCondition(getSQ_Detail(), 
                                                    "sTransNox = " + SQLUtil.toSQL(fsTransNox)));
               
        for (int lnCtr = 1; lnCtr <= MiscUtil.RecordCount(loRS); lnCtr ++){
            loRS.absolute(lnCtr);
            
            loOcc = new UnitPODetail();
            
            for(int lnCol=1; lnCol<=loRS.getMetaData().getColumnCount(); lnCol++){
                loOcc.setValue(lnCol, loRS.getObject(lnCol));
            }
            loDetail.add(loOcc);
        }
        
        return loDetail;
    }
    
    private String getSQ_Detail(){
        return "SELECT" +
                    "  sTransNox" +
                    ", nEntryNox" + 
                    ", sStockIDx" + 
                    ", nQuantity" + 
                    ", nUnitPrce" + 
                    ", nReceived" + 
                    ", nCancelld" + 
                    ", dModified" + 
                " FROM " + pxeDetTable +
                " ORDER BY nEntryNox";
    }
    
    //Added methods
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
    
    private ArrayList<UnitPODetail> poDetail;
    private ArrayList<UnitPOReceivingDetailOthers> paDetailOthers;
    private final String pxeMasTable = "PO_Master";
    private final String pxeDetTable = "PO_Detail";

    @Override
    public boolean closeTransaction(String string) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}