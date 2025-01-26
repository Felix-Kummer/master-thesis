package federatedSim;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

public class ConfigParser {

    // TODO add all params from federatedTwoSites
    private String workflowPath;
    private int numSites;
    private String strategy;
    private int taskThreshold;
    private int secThreshold;


    private HashMap<Integer, Site> id2site = new HashMap<>();

    public void parse(String path) {
        FileReader reader;
        try {
            reader = new FileReader(path);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        JsonElement jsonElement = JsonParser.parseReader(reader);
        JsonObject jsonObject   = jsonElement.getAsJsonObject();


        // ### PARSE GENERAL INFORMATION ###
        workflowPath  = jsonObject.get("workflowPath").getAsString();
        numSites      = jsonObject.get("numSites").getAsInt();
        strategy      = jsonObject.get("strategy").getAsString();
        taskThreshold = jsonObject.get("taskThreshold").getAsInt();
        secThreshold  = jsonObject.get("secThreshold").getAsInt();


        // ### PARSE SITES ###
        JsonArray jsonArraySites = jsonObject.getAsJsonArray("sites");
        for (JsonElement siteElement : jsonArraySites ) {
            JsonObject siteObj = siteElement.getAsJsonObject();
            int id = siteObj.get("id").getAsInt();
            Site site = new Site(siteObj);
            id2site.put(id, site);
        }

        // ### PARSE FILES ###
        JsonArray jsonArrayFiles = jsonObject.getAsJsonArray("files");
        for (JsonElement filesElement : jsonArrayFiles ) {
            JsonObject fileObj = filesElement.getAsJsonObject();
            int siteId = fileObj.get("siteId").getAsInt();
            Site site = id2site.get(siteId);
            File file = new File(fileObj);
            site.addFile(file);
        }

        // ### PARSE CONNECTIONS ###
        JsonArray jsonArrayConnections = jsonObject.getAsJsonArray("connections");
        for (JsonElement connElement : jsonArrayConnections) {
            JsonObject connObj = connElement.getAsJsonObject();
            int site1Id      = connObj.get("site1Id").getAsInt();
            int site2Id      = connObj.get("site2Id").getAsInt();
            double bandwidth = connObj.get("bandwidth").getAsDouble();

            Site site1 = id2site.get(site1Id);
            Site site2 = id2site.get(site2Id);

            site1.addConnection(site2Id, bandwidth);
            site2.addConnection(site1Id, bandwidth);
        }

    }

    // getters
    public String getWorkflowPath() {
        return workflowPath;
    }
    public int getNumSites() { return numSites; }
    public String getStrategy() { return strategy; }
    public Map<Integer, Site> getDatacentersSpecs() { return id2site; }
    public int getSecThreshold() { return secThreshold; }
    public int getTaskThreshold() { return taskThreshold; }

    public class Site {
        private Integer id;
        private double storage;
        private int ram;
        private int mips;
        private int pes;
        private double intraBw;

        // maps other siteId to bandwidth with this site
        private Map<Integer, Double> otherId2Bandwidth = new HashMap<>();

        private ArrayList<File> files = new ArrayList<File>();

        public Site(JsonObject site) {

            id      = site.get("id").getAsInt();
            storage = site.get("storage").getAsDouble();
            ram     = site.get("ram").getAsInt();
            mips    = site.get("mips").getAsInt();
            pes     = site.get("pes").getAsInt();
            intraBw = site.get("intraBw").getAsDouble();

        }

        protected void addFile(File file) {
            files.add(file);
        }

        protected void addConnection(int otherId, double bandwidth) {
            if (otherId2Bandwidth.containsKey(otherId)) {
                throw new RuntimeException("ConfigParser tried to configure same bandwidth more than once");
            }

            otherId2Bandwidth.put(otherId, bandwidth);
        }

        // getters
        public ArrayList<File> getFiles() {
            return files;
        }
        public double getStorage() { return storage; }
        public int getRam() { return ram; }
        public int getMips() { return mips; }
        public int getPes() { return pes; }
        public Map<Integer, Double> getConnectionMap() { return otherId2Bandwidth; }
        public double getIntraBw() { return intraBw; }
    }

    public class File {

        Integer size;
        String name;


        public Integer getSize() {
            return size;
        }
        public String getName() {
            return name;
        }



        public File(JsonObject fileObj) {
            size = fileObj.get("size").getAsInt();
            name = fileObj.get("name").getAsString();
        }
    }
}
