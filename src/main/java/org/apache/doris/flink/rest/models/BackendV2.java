package org.apache.doris.flink.rest.models;

import org.apache.doris.shaded.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.doris.shaded.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * @author winn-bmt
 * Date: 2024/1/26 11:50
 * Description:
 *      由于在服务器部署doris时，服务器间使用的都是内网地址
 *      连接器通过公网向fe请求be的地址时，fe返回的也是be的内网地址
 *      这样会导致在本地ide测试时，导致获取不到be。
 *      解决办法：62行填上be对应的公网地址即可
 *      -- 如果没有上述问题，将此文件注释掉即可
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BackendV2 {


    @JsonProperty(value = "backends")
    private List<BackendRowV2> backends;

    public List<BackendRowV2> getBackends() {
        return backends;
    }

    public void setBackends(List<BackendRowV2> backends) {
        this.backends = backends;
    }

    public static class BackendRowV2 {
        public String ip;
        public int http_port;
        public boolean is_alive;

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public int getHttpPort() {
            return http_port;
        }

        public void setHttpPort(int httpPort) {
            this.http_port = httpPort;
        }

        public boolean isAlive() {
            return is_alive;
        }

        public void setAlive(boolean alive) {
            is_alive = alive;
        }

        public String toBackendString(){
            return "公网ip" + ":" + http_port;
        }

    }
}
