package federatedSim;

public class StorageException extends Exception {
    private double size;

    public StorageException(double size) {
        super("Size " + size + " exceeded capacity storage capacity");
        this.size = size;
    }

    public double getSize() {
        return this.size;
    }
}
