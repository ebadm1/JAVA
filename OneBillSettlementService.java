package com.secp.leap.leapintegration.services;

import com.jcraft.jsch.*;
import com.secp.leap.leapintegration.domains.OneBillSettlementDomain;
import com.secp.leap.leapintegration.models.ErpDataResponseModel;
import com.secp.leap.leapintegration.models.TransactionRequest;
import com.secp.leap.leapintegration.repository.OneBillSettlementRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;


@Service
public class OneBillSettlementService {

    @Autowired
    OneBillSettlementRepository oneBillSettlementRepository;
    @Value("${sftp.host}")
    private String sftpHost;
    @Value("${sftp.port}")
    private int sftpPort;
    @Value("${sftp.username}")
    private String sftpUsername;
    @Value("${sftp.password}")
    private String sftpPassword;
    @Value("${sftp.remoteFilePathOneBill}")
    private String remoteFilePath;
    private static final Logger logger = LoggerFactory
            .getLogger(OneBillSettlementService.class);

    public boolean OneBillFileParser() {
        try {
            Authentication auth = SecurityContextHolder.getContext().getAuthentication();
            OffsetDateTime now = OffsetDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            String currentDate = now.format(formatter);
            logger.info(OneBillSettlementService.class + " - Connecting to SFTP server");
            JSch jsch = new JSch();
            Session session = jsch.getSession(sftpUsername, sftpHost, sftpPort);
            session.setPassword(sftpPassword);
            session.setConfig("StrictHostKeyChecking", "no"); // disable host key checking
            session.connect();
            logger.info(OneBillSettlementService.class + " - Connected to SFTP server");
            ChannelSftp sftpChannel = (ChannelSftp) session.openChannel("sftp");
            sftpChannel.connect();
            logger.info(OneBillSettlementService.class + " - SFTP Channel Established");
            SftpATTRS attrs = null;
            try {
                attrs = sftpChannel.stat(remoteFilePath);
                logger.info(OneBillSettlementService.class + " - Found directory - " + remoteFilePath);
            } catch (SftpException e) {
                e.printStackTrace();
                logger.info(OneBillSettlementService.class + " - Directory not found - " + remoteFilePath);
            }
            if (attrs == null || !attrs.isDir()) {
                logger.info(OneBillSettlementService.class + " - No such directory found");
                return false;
            }else {
                sftpChannel.cd(remoteFilePath);
                Vector<ChannelSftp.LsEntry> fileList = sftpChannel.ls(remoteFilePath);
                logger.info(OneBillSettlementService.class + " - List of files is SFTP Directory - " + fileList);
                if (fileList == null || fileList.isEmpty()) {
                    logger.info(OneBillSettlementService.class + " - No files found in the SFTP directory.");
                    return true;
                }
                boolean hasFiles = false;
                int count = 1;
                for (ChannelSftp.LsEntry file : fileList) {
                    Set<String> existingConsumerNos = oneBillSettlementRepository.findAllConsumerNos();

                    if (!file.getAttrs().isDir() && file.getFilename().endsWith(".txt")) {
                        String fileName = file.getFilename();
                        InputStream inputStream = sftpChannel.get(fileName);
                        logger.info(OneBillSettlementService.class + " - Reading file - " + fileName);
                        try {
                            hasFiles = true;
                            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                            String st;
                            List<OneBillSettlementDomain> oneBillSettlementDomainList = new ArrayList<>();
                            while ((st = br.readLine()) != null)
                            {
                                String consumerNo = st.substring(8, 28).trim();
                                if (existingConsumerNos.contains(consumerNo)) {
                                    logger.warn(OneBillSettlementService.class + " - Skipping duplicate ConsumerNo - " + consumerNo);
                                    continue;
                                }
                                String amountPaid = st.substring(85, 99);
                                amountPaid = amountPaid.replaceFirst("^0*", "");
                                StringBuilder amountPaidFormatted = new StringBuilder(amountPaid);
                                amountPaidFormatted.insert(amountPaidFormatted.length() - 2, ".");
                                OneBillSettlementDomain oneBillSettlementDomain = new OneBillSettlementDomain();
                                oneBillSettlementDomain.setUtilityCompanyAccount(st.substring(65, 85).trim());
                                oneBillSettlementDomain.setUtilityCompanyId(st.substring(0, 8).trim());
                                oneBillSettlementDomain.setConsumerNo(st.substring(8, 28).trim());
                                oneBillSettlementDomain.setAIID(st.substring(28, 34).trim());
                                oneBillSettlementDomain.setAIID2(st.substring(34, 40).trim());
                                oneBillSettlementDomain.setPayerId(st.substring(40, 65).trim());
                                oneBillSettlementDomain.setAmountPaid(String.valueOf(amountPaidFormatted));
                                oneBillSettlementDomain.setDatePaid(st.substring(99, 107).trim());
                                oneBillSettlementDomain.setTimePaid(st.substring(107, 113).trim());
                                oneBillSettlementDomain.setDateSettlement(st.substring(113, 121).trim());
                                oneBillSettlementDomain.setPaymentMode(st.substring(121, 122).trim());
                                oneBillSettlementDomain.setBankName(st.substring(122, 128).trim());
                                oneBillSettlementDomain.setAuthId(st.substring(128, 134).trim());
                                oneBillSettlementDomain.setSTAN(st.substring(134, 140).trim());
                                oneBillSettlementDomain.setFileName(fileName);
                                oneBillSettlementDomain.setBatchNumber("1Bill - Daily - Cleared SECP - " + currentDate);
                                oneBillSettlementDomain.setCreatedDate(java.sql.Date.valueOf(LocalDate.now()));
                                oneBillSettlementDomain.setCreatedBy(auth.getName());
                                oneBillSettlementDomain.setModifyBy(auth.getName());
                                oneBillSettlementDomain.setModifyDate(java.sql.Date.valueOf(LocalDate.now()));
                                oneBillSettlementDomain.setIsError(false);
                                oneBillSettlementDomainList.add(oneBillSettlementDomain);
                            }
                            oneBillSettlementRepository.saveAll(oneBillSettlementDomainList.stream().distinct().collect(Collectors.toList()));
                            oneBillSettlementDomainList.clear();
                            inputStream.close();
                            logger.info(OneBillSettlementService.class + " - File parse successfully - " + fileName);
                            try {
                                logger.info(OneBillSettlementService.class + " - Trying to create Archived folder if not Present");
                                sftpChannel.mkdir("archive");
                            } catch (com.jcraft.jsch.SftpException e) {
                                logger.info(OneBillSettlementService.class + " - Archived Directory already exist");
                                if (e.id != ChannelSftp.SSH_FX_FAILURE) {
                                    e.printStackTrace();
                                }
                            }
                            String formattedDate = new java.text.SimpleDateFormat("yyyy-MM-dd-mm-ss").format(new java.util.Date());
                            String newFilename = "1Bill-" + formattedDate + " " + count + ".txt";
                            logger.info(OneBillSettlementService.class + " - Rename File - " + newFilename);
                            sftpChannel.rename(fileName, newFilename);
                            logger.info(OneBillSettlementService.class + " - Moving file to archived");
                            sftpChannel.rename(newFilename, "archive/" + newFilename);
                            logger.info(CardsSettlementService.class + " - Check Challans in Application");
                            verifyOneBillChallanInApplication();
                        } catch (Exception e) {
                            logger.info(OneBillSettlementService.class + " - Is not a 1Bill Settlement File");
                            inputStream.close();
                        }
                        count++;
                    }
                }
                logger.info(OneBillSettlementService.class + " - No .txt files found in the SFTP directory.");
                sftpChannel.disconnect();
                session.disconnect();
                return true;
            }
        } catch (Exception e) {
            logger.info(OneBillSettlementService.class + " - SMTP connection error");
            e.printStackTrace();
            return false;
        }
    }

    public Optional<List<OneBillSettlementDomain>> getSettlementData() {
        //get current date
        DateFormat dform = new SimpleDateFormat("yyyyddMM");
        Date obj = new Date();
        String date = dform.format(obj);
        Optional<List<OneBillSettlementDomain>> settlementData = oneBillSettlementRepository.findByDateSettlement(date);


        System.out.println(dform.format(obj));
        return settlementData;
    }

    public List<String> getOneBillChallans(){
        List<String> filenames = oneBillSettlementRepository.findDistinctFilenames();
        if(filenames.isEmpty())
            return new ArrayList<>();
        List<OneBillSettlementDomain> challan = oneBillSettlementRepository.findByPostedErpFalse(filenames.get(0));
        Set<String> uniqueConsumerNumbers = new HashSet<>();
        for (OneBillSettlementDomain settlement : challan) {
            String consumerNumber = settlement.getConsumerNo();
            uniqueConsumerNumbers.add(consumerNumber);
        }
        return new ArrayList<>(uniqueConsumerNumbers);
    }

    public List<TransactionRequest> getErpData(List<String> consumerNumbers){
        OffsetDateTime now = OffsetDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter transactionDate = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSS XXX");
        String formattedDate = now.format(formatter);
        String formattedTransactionDate = now.format(transactionDate);
        List<Object[]> results = oneBillSettlementRepository.findTotalByConsumerNumbersIn(consumerNumbers);
        List<ErpDataResponseModel> response = new ArrayList<>();
        for (Object[] result : results) {
            ErpDataResponseModel model = new ErpDataResponseModel(
                    result[0].toString(),
                    result[1].toString(),
                    result[2].toString(),
                    (BigDecimal) result[3],
                    (BigDecimal) result[3],
                    formattedDate
            );
            response.add(model);
        }

        TransactionRequest transactionRequest = new TransactionRequest();
        transactionRequest.setBatch_id(oneBillSettlementRepository.getNextSequenceValue().toString());
        transactionRequest.setBatch_name("Daily - Cleared SECP - " + formattedDate);
        transactionRequest.setBatch_type("Daily Batch");
        transactionRequest.setJournal_category("E-Challan");
        transactionRequest.setAccounting_datetime(formattedTransactionDate);
        transactionRequest.setFields(response);

        List<TransactionRequest> transactionRequestList = new ArrayList<>();
        transactionRequestList.add(transactionRequest);

        return transactionRequestList;

    }

    public boolean updateChallanNumbers(List<String> consumerNumbers){
        try {
            oneBillSettlementRepository.markAsPostedToErp(consumerNumbers);
            return true;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void verifyOneBillChallanInApplication(){
        oneBillSettlementRepository.updateOnebillSettlementData();
    }

}
