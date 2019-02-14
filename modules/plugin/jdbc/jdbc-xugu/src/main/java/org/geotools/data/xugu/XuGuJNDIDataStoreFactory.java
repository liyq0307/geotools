package org.geotools.data.xugu;

import org.geotools.jdbc.JDBCJNDIDataStoreFactory;

/**
 * Created by liyq on 2018/11/1.
 */
public class XuGuJNDIDataStoreFactory extends JDBCJNDIDataStoreFactory {
    public XuGuJNDIDataStoreFactory() {
        super(new XuGuDataStoreFactory());
    }
}
