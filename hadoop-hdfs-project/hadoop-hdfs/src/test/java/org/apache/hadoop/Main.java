package org.apache.hadoop;

import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.BPServiceActor;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.TestFsDatasetImpl;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;

/**
 * @author pengwang
 * @date 2020/06/04
 */
public class Main {

    public static void main(String[] args) {
        Storage.StorageDirectory storageDirectory;
        DataStorage datanodeStorage;
        TestFsDatasetImpl fsDataset;
        LeaseManager leaseManager;
    }
}