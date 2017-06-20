package es.bsc.mobile.apps.ced;

import java.io.DataInputStream;
import java.io.IOException;

public class Operations {

    public static byte[] initialized(byte[] data, float[][] gaus, int[][] sobx, int[][] soby) {
        return new byte[]{0};
    }

    public static byte[] gaussian(final byte[] data, final float[][] gaus, final int rows, final int cols) {
        byte[] out = new byte[rows * cols];
        for (int row = 1; row < rows - 1; row++) {
            for (int col = 1; col < cols - 1; col++) {
                int sum = 0;
                for (int i = 0; i < 3; i++) {
                    for (int j = 0; j < 3; j++) {
                        sum += gaus[i][j] * (0xFF & data[(i + row + -1) * cols + (j + col + -1)]);
                    }
                }
                out[row * cols + col] = (byte) Math.min(255, Math.max(0, sum));
            }
        }
        return out;
    }

    public static byte[] sobel(final byte[] data, final int rows, final int cols, final int[][] sobx, final int[][] soby) {
        byte[] theta = new byte[2 * rows * cols];
        float PI = 3.14159265f;
        int thetaOffset = rows;
        for (int row = 1; row < rows - 1; row++) {
            for (int col = 1; col < cols - 1; col++) {
                double sumx = 0;
                double sumy = 0;
                double angle = 0;

                // find x and y derivatives
                for (int i = 0; i < 3; i++) {
                    for (int j = 0; j < 3; j++) {
                        sumx += sobx[i][j] * (0xFF & data[(i + row + -1) * cols + (j + col + -1)]);
                        sumy += soby[i][j] * (0xFF & data[(i + row + -1) * cols + (j + col + -1)]);
                    }
                }

                // The output is now the square root of their squares, but they are
                // constrained to 0 <= value <= 255. Note that hypot is a built in function
                // defined as: hypot(x,y) = sqrt(x*x, y*y).
                theta[(row + thetaOffset) * cols + col] = (byte) Math.min(255, Math.max(0, Math.hypot(sumx, sumy)));
                // Compute the direction angle theta in radians
                // atan2 has a range of (-PI, PI) degrees
                angle = Math.atan2(sumy, sumx);

                // If the angle is negative,
                // shift the range to (0, 2PI) by adding 2PI to the angle,
                // then perform modulo operation of 2PI
                if (angle < 0) {
                    angle = (angle + 2 * PI) % (2 * PI);
                }

                // Round the angle to one of four possibilities: 0, 45, 90, 135 degrees
                // then store it in the theta buffer at the proper position
                if (angle <= PI / 8) {
                    theta[row * cols + col] = 0;
                } else if (angle <= 3 * PI / 8) {
                    theta[row * cols + col] = 45;
                } else if (angle <= 5 * PI / 8) {
                    theta[row * cols + col] = 90;
                } else if (angle <= 7 * PI / 8) {
                    theta[row * cols + col] = (byte) 135;
                } else if (angle <= 9 * PI / 8) {
                    theta[row * cols + col] = 0;
                } else if (angle <= 11 * PI / 8) {
                    theta[row * cols + col] = 45;
                } else if (angle <= 13 * PI / 8) {
                    theta[row * cols + col] = 90;
                } else if (angle <= 15 * PI / 8) {
                    theta[row * cols + col] = (byte) 135;
                } else {
                    theta[row * cols + col] = 0; // (angle <= 16*PI/8)
                }
            }
        }
        return theta;
    }

    public static byte[] nonMaxSupp(final byte[] data, final int rows, final int cols) {
        byte[] out = new byte[rows * cols];
        for (int row = 1; row < rows - 1; row++) {
            for (int col = 1; col < cols - 1; col++) {
                // These variables are offset by one to avoid seg. fault errors
                // As such, this kernel ignores the outside ring of pixels
                int pos = row * cols + col;
                int value = 0xFF & data[(rows + row) * cols + col];
                
                int east = 0xFF & data[(rows + row) * cols + col + 1];
                int west = 0xFF & data[(rows + row) * cols + col - 1];

                int north = 0xFF & data[(rows + row - 1) * cols + col];
                int northeast = 0xFF & data[(rows + row - 1) * cols + col + 1];
                int northwest = 0xFF & data[(rows + row - 1) * cols + col - 1];

                int south = 0xFF & data[(rows + row + 1) * cols + col];
                int southeast = 0xFF & data[(rows + row + 1) * cols + col + 1];
                int southwest = 0xFF & data[(rows + row + 1) * cols + col - 1];

                switch (data[pos] & 0xFF) {
                    // A gradient angle of 0 degrees = an edge that is North/South
                    // Check neighbors to the East and West
                    case 0:
                        // supress me if my neighbor has larger magnitude
                        if (value <= east || value <= west) {
                            out[pos] = 0;
                        } // otherwise, copy my value to the output buffer
                        else {
                            out[pos] = (byte) value;
                        }
                        break;

                    // A gradient angle of 45 degrees = an edge that is NW/SE
                    // Check neighbors to the NE and SW
                    case 45:
                        // supress me if my neighbor has larger magnitude
                        if (value <= northeast || (value) <= southwest) {
                            out[pos] = 0;
                        } // otherwise, copy my value to the output buffer
                        else {
                            out[pos] = (byte) value;
                        }
                        break;

                    // A gradient angle of 90 degrees = an edge that is E/W
                    // Check neighbors to the North and South
                    case 90:
                        // supress me if my neighbor has larger magnitude
                        if (value <= north || value <= south) {
                            out[pos] = 0;
                        } // otherwise, copy my value to the output buffer
                        else {
                            out[pos] = (byte) value;
                        }
                        break;

                    // A gradient angle of 135 degrees = an edge that is NE/SW
                    // Check neighbors to the NW and SE
                    case 135:
                        // supress me if my neighbor has larger magnitude
                        if ((value) <= northwest || (value) <= southeast) {
                            out[pos] = 0;
                        } // otherwise, copy my value to the output buffer
                        else {
                            out[pos] = (byte) value;
                        }
                        break;

                    default:
                        out[pos] = (byte) value;
                        break;
                }
            }
        }
        return out;
    }

    public static byte[] hysteresis(final byte[] data, final int rows, final int cols) {
        byte[] out = new byte[rows * cols];
        // Establish our high and low thresholds as floats
        float lowThresh = 10;
        float highThresh = 70;
        byte EDGE = (byte) 255;
        for (int row = 1; row < rows - 1; row++) {
            for (int col = 1; col < cols - 1; col++) {
                if ((0xFF & data[row * cols + col]) >= highThresh) {
                    out[row * cols + col] = EDGE;
                } else if ((0xFF & data[row * cols + col]) <= lowThresh) {
                    out[row * cols + col] = 0;
                } else {
                    float med = (highThresh + lowThresh) / 2;

                    if ((0xFF & data[row * cols + col]) >= med) {
                        out[row * cols + col] = EDGE;
                    } else {
                        out[row * cols + col] = 0;
                    }
                }
            }
        }
        return out;
    }

    public static int validate(byte[] frame, int rows, int cols, DataInputStream dis) throws IOException {
        int count_error = 0;
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                byte pix = dis.readByte();
                if (frame[row * cols + col] != pix) {
                    count_error++;
                }
            }
        }
        return count_error;
    }
}
