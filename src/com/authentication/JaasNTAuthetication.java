package com.authentication;

import java.awt.AWTEvent;
import java.awt.Frame;
import java.awt.Toolkit;
import java.awt.Window;
import java.awt.event.AWTEventListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

public class JaasNTAuthetication {
	
	static{
		System.setProperty("java.security.auth.login.config", "C:\\designtools\\workspace\\test\\parallel\\src\\com\\authentication\\jaas.config");
	}
	
	static class EraserThread implements Runnable {
		   private boolean stop;
		 
		   /**
		    *@param The prompt displayed to the user
		    */
		   public EraserThread(String prompt) {
		       System.out.print(prompt);
		   }

		   /**
		    * Begin masking...display asterisks (*)
		    */
		   public void run () {
		      stop = true;
		      while (stop) {
		         System.out.print("\010*");
			 try {
			    Thread.sleep(1);
		         } catch(InterruptedException ie) {
		            ie.printStackTrace();
		         }
		      }
		   }

		   /**
		    * Instruct the thread to stop masking
		    */
		   public void stopMasking() {
		      this.stop = false;
		   }
		}

	public static void authenticate(){
		CallbackHandler cbh = new CallbackHandler() {
			
			@Override
			public void handle(Callback[] callbacks) throws IOException,
					UnsupportedCallbackException {
				for (int i = 0; i < callbacks.length; i++) {
			        if (callbacks[i] instanceof NameCallback) {
			            NameCallback nameCb = (NameCallback)callbacks[i];
			            System.out.print(nameCb.getPrompt());
			            String user=(new BufferedReader(new InputStreamReader(System.in))).readLine();
			            nameCb.setName(user);
			        } else if (callbacks[i] instanceof PasswordCallback) {
			            final PasswordCallback passCb = (PasswordCallback)callbacks[i];
			            
			            String pass;
			            System.out.print(passCb.getPrompt());
			            if(passCb.isEchoOn()){
			            	
			            	pass=(new BufferedReader(new InputStreamReader(System.in))).readLine();
			            	passCb.setPassword(pass.toCharArray());
			            }
			            else{
			            	final StringBuffer buff = new StringBuffer();
			            	final PrintWriter out = new PrintWriter(System.out);
			            	final CountDownLatch latch = new CountDownLatch(1);
			            	
			            	Toolkit.getDefaultToolkit().addAWTEventListener(new AWTEventListener() {
								
								@Override
								public void eventDispatched(AWTEvent event) {
									System.out.println(event.getID());
									
								}
							}, AWTEvent.KEY_EVENT_MASK | AWTEvent.ACTION_EVENT_MASK);
							
							
			                /*final Frame f = new Frame();
			                f.setVisible(false);
			                f.
			                f.addKeyListener(new KeyListener() {
								
								@Override
								public void keyTyped(KeyEvent e) {
									
									
								}
								
								@Override
								public void keyReleased(KeyEvent e) {
									// TODO Auto-generated method stub
									
								}
								
								@Override
								public void keyPressed(KeyEvent e) {
									char c = e.getKeyChar();
									
									if(c == '\r' || c == '\n'){
										passCb.setPassword(buff.toString().toCharArray());
										f.setVisible(false);
										f.dispose();
										latch.countDown();
									}
									else{
										buff.append(c);
										out.print('*');
									}
									
								}
							});*/
			                
			                try {
								latch.await();
							} catch (InterruptedException e1) {
								
							}
			                			            	
			            }
			            
			        } else {
			            throw(new UnsupportedCallbackException(callbacks[i], "Callback class not supported"));
			        }
			    }

				
			}
		};
		   LoginContext lc;
		try {
			lc = new LoginContext("NT", cbh);
			lc.login();
			System.out.println("Login success!!");
		} catch (LoginException e) {
			System.err.println("Login failed!!");
			e.printStackTrace();
		}
		   

	}
	
	public static void main(String [] s){
		authenticate();
	}

}
