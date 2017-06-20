package es.bsc.mobile.apps.bs;

public class Params {

    private final int nTileSize = 16;
    private final int nWarmup = 5;
    private final int inRows = 3;
    private final int inCols = 3;
    private final int outSizeI = 512;
    private final int outSizeJ = 512;
    
    public int getInRows() {
        return inRows;
    }

    public int getInCols() {
        return inCols;
    }

    public int getOutRows() {
        return outSizeI;
    }

    public int getOutCols() {
        return outSizeJ;
    }

    public int getnWarmup() {
        return nWarmup;
    }

    public int getTileSize() {
        return nTileSize;
    }

}
