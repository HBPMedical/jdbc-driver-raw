package raw.jdbc.rawclient;


import java.io.IOException;

public class RawClientException extends IOException{
    public RawClientException(){}

    public RawClientException(String message){
        super(message);
    }
}
