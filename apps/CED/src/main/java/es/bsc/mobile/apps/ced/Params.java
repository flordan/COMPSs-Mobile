/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.bsc.mobile.apps.ced;

/**
 *
 * @author flordan
 */
public class Params {

    private int nWorkItems = 16;
    private int nThreads = 4;
    private int nWarmup = 20;
    private int reps = 10;

    public Params() {
    }

    public int getnWorkItems() {
        return nWorkItems;
    }

    public int getnThreads() {
        return nThreads;
    }

    public int getnWarmup() {
        return nWarmup;
    }

    public int getReps() {
        return reps;
    }

}
