package com.neovaryag.kafkatool.ui;

import com.neovaryag.kafkatool.backend.Kafka;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.swing.*;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mihailsimonov
 */
@Slf4j
@Component
public class JFrame extends javax.swing.JFrame {

    public static final String BOOTSTRAP_SERVERS_TXT = "bootstrapServers.txt";
    private final Kafka kafka;

    @Autowired
    public JFrame(Kafka kafka) {
        this.kafka = kafka;
        initComponents();
    }

    private void initComponents() {

        jGroupId = new JTextField();
        jListTopicsTo = new JComboBox<>();
        jListPorts = new JComboBox<>();
        JButton jPut = new JButton();
        JButton jGet = new JButton();
        JScrollPane jScrollPane1 = new JScrollPane();
        jTextArea1 = new JTextArea();
        JScrollPane jScrollPane2 = new JScrollPane();
        jTextArea2 = new JTextArea();
        jToggleButtonSsl = new JToggleButton();
        JLabel jLabel1 = new JLabel();
        JLabel jLabel2 = new JLabel();
        jListTopicsFrom = new JComboBox<>();
        // Variables declaration - do not modify
        JButton jClear = new JButton();
        jListServers = new JComboBox<>();
        JButton refreshGetTopicsByServerName = new JButton();
        JLabel jOutput = new JLabel();


        jGroupId.setText("groupId");
        jGroupId.addActionListener(this::jGroupIdActionPerformed);

        jListTopicsTo.setEditable(true);
        jListTopicsTo.setModel(new DefaultComboBoxModel<>(new String[]{"from topics"}));
        jListTopicsTo.addActionListener(evt -> jListTopicsToActionPerformed());

        jListPorts.setEditable(true);
        jListPorts.setModel(new DefaultComboBoxModel<>(new String[]{"9092"}));
        jListPorts.addActionListener(evt -> jListPortsActionPerformed());

        jPut.setText("PUT");
        jPut.addActionListener(evt -> jPutActionPerformed());

        jGet.setText("GET");
        jGet.addActionListener(evt -> jGetActionPerformed());

        jTextArea1.setColumns(20);
        jTextArea1.setRows(5);
        jScrollPane1.setViewportView(jTextArea1);

        jTextArea2.setEditable(false);
        jTextArea2.setColumns(20);
        jTextArea2.setRows(5);
        jTextArea2.setDebugGraphicsOptions(DebugGraphics.BUFFERED_OPTION);
        jScrollPane2.setViewportView(jTextArea2);

        jToggleButtonSsl.setText("SSL");
        jToggleButtonSsl.addActionListener(evt -> jToggleButtonSsl.isSelected());

        jLabel1.setForeground(new java.awt.Color(153, 153, 153));
        jLabel1.setText("© Simonov Mikhail");

        jLabel2.setText("input");

        jListTopicsFrom.setEditable(true);
        jListTopicsFrom.setModel(new DefaultComboBoxModel<>(new String[]{"to topics"}));
        jListTopicsFrom.addActionListener(evt -> jListTopicsFromActionPerformed());

        jClear.setText("CLEAR");
        jClear.addActionListener(evt1 -> jClearActionPerformed());

        jListServers.setEditable(true);
        jListServers.addActionListener(evt -> jListServersActionPerformed());

        saveOrUpdate();

        refreshGetTopicsByServerName.setText("REFRESH");
        refreshGetTopicsByServerName.addActionListener(evt -> refreshGetTopicsByServerNameActionPerformed());

        jOutput.setText("output");

        GroupLayout layout = new GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
                layout.createParallelGroup(GroupLayout.Alignment.LEADING)
                        .addGroup(GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                                .addGroup(layout.createParallelGroup(GroupLayout.Alignment.TRAILING)
                                        .addGroup(layout.createSequentialGroup()
                                                .addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                                .addComponent(jLabel1))
                                        .addGroup(GroupLayout.Alignment.LEADING, layout.createSequentialGroup()
                                                .addGap(52, 52, 52)
                                                .addGroup(layout.createParallelGroup(GroupLayout.Alignment.LEADING)
                                                        .addComponent(jScrollPane2)
                                                        .addGroup(layout.createSequentialGroup()
                                                                .addGroup(layout.createParallelGroup(GroupLayout.Alignment.LEADING)
                                                                        .addComponent(jListTopicsFrom, GroupLayout.PREFERRED_SIZE, 212, GroupLayout.PREFERRED_SIZE)
                                                                        .addGroup(layout.createSequentialGroup()
                                                                                .addComponent(jListServers, GroupLayout.PREFERRED_SIZE, 212, GroupLayout.PREFERRED_SIZE)
                                                                                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                                                                .addComponent(jListPorts, GroupLayout.PREFERRED_SIZE, 62, GroupLayout.PREFERRED_SIZE)
                                                                                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                                                                .addComponent(refreshGetTopicsByServerName, GroupLayout.PREFERRED_SIZE, 82, GroupLayout.PREFERRED_SIZE))
                                                                        .addComponent(jGroupId, GroupLayout.PREFERRED_SIZE, 212, GroupLayout.PREFERRED_SIZE)
                                                                        .addComponent(jListTopicsTo, GroupLayout.PREFERRED_SIZE, 212, GroupLayout.PREFERRED_SIZE))
                                                                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                                                .addGroup(layout.createParallelGroup(GroupLayout.Alignment.TRAILING, false)
                                                                        .addComponent(jToggleButtonSsl, GroupLayout.DEFAULT_SIZE, 120, Short.MAX_VALUE)
                                                                        .addComponent(jPut, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                                                                .addGap(18, 18, 18)
                                                                .addComponent(jGet, GroupLayout.PREFERRED_SIZE, 120, GroupLayout.PREFERRED_SIZE))
                                                        .addGroup(layout.createSequentialGroup()
                                                                .addComponent(jOutput)
                                                                .addGap(0, 896, Short.MAX_VALUE))
                                                        .addComponent(jScrollPane1, GroupLayout.Alignment.TRAILING)
                                                        .addGroup(layout.createSequentialGroup()
                                                                .addComponent(jLabel2)
                                                                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                                                .addComponent(jClear)))))
                                .addGap(55, 55, 55))
        );
        layout.setVerticalGroup(
                layout.createParallelGroup(GroupLayout.Alignment.LEADING)
                        .addGroup(layout.createSequentialGroup()
                                .addGap(48, 48, 48)
                                .addGroup(layout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(jListPorts, GroupLayout.PREFERRED_SIZE, 37, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(jPut, GroupLayout.PREFERRED_SIZE, 37, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(jGet, GroupLayout.PREFERRED_SIZE, 36, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(jListServers, GroupLayout.PREFERRED_SIZE, 35, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(refreshGetTopicsByServerName, GroupLayout.PREFERRED_SIZE, 38, GroupLayout.PREFERRED_SIZE))
                                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                .addGroup(layout.createParallelGroup(GroupLayout.Alignment.BASELINE)
                                        .addComponent(jGroupId, GroupLayout.PREFERRED_SIZE, 35, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(jToggleButtonSsl, GroupLayout.PREFERRED_SIZE, 35, GroupLayout.PREFERRED_SIZE))
                                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                .addComponent(jListTopicsTo, GroupLayout.PREFERRED_SIZE, 35, GroupLayout.PREFERRED_SIZE)
                                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                .addComponent(jListTopicsFrom, GroupLayout.PREFERRED_SIZE, 35, GroupLayout.PREFERRED_SIZE)
                                .addGroup(layout.createParallelGroup(GroupLayout.Alignment.LEADING)
                                        .addGroup(GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                                                .addComponent(jClear)
                                                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED))
                                        .addGroup(layout.createSequentialGroup()
                                                .addGap(16, 16, 16)
                                                .addComponent(jLabel2)
                                                .addGap(3, 3, 3)))
                                .addComponent(jScrollPane1, GroupLayout.PREFERRED_SIZE, 209, GroupLayout.PREFERRED_SIZE)
                                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
                                .addComponent(jOutput)
                                .addGap(3, 3, 3)
                                .addComponent(jScrollPane2, GroupLayout.PREFERRED_SIZE, 359, GroupLayout.PREFERRED_SIZE)
                                .addPreferredGap(LayoutStyle.ComponentPlacement.RELATED, 32, Short.MAX_VALUE)
                                .addComponent(jLabel1)
                                .addContainerGap())
        );

        pack();
    }

    private void saveOrUpdate() {
        String fileName = "bootstrapServers.txt";
        BufferedReader input = null;
        try {
            input = new BufferedReader(new FileReader(fileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        List<String> ips = new ArrayList<>();
        List<String> ports = new ArrayList<>();

        onAdresess(input, ips, ports);

        ips.stream().distinct().toList().forEach(s -> jListServers.addItem(s));
        ports.stream().distinct().toList().forEach(s -> jListPorts.addItem(s));
    }

    private void onAdresess(BufferedReader input, List<String> ips, List<String> ports) {
        try {
            String line;
            while ((line = Objects.requireNonNull(input).readLine()) != null) {
                String[] ip = line.split(":");
                String sIp = ip[0];
                ips.add(sIp);

                String[] port = line.split(":");
                String sPort = port[1];
                ports.add(sPort);
            }
        } catch (IOException e) {
            log.error("Error, file didn't exist.");
        } finally {
            try {
                Objects.requireNonNull(input).close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void jGroupIdActionPerformed(java.awt.event.ActionEvent evt) {
        // TODO add your handling code here:
    }

    private void jListTopicsToActionPerformed() {
    }

    private void jListPortsActionPerformed() {
    }

    private void jPutActionPerformed() {
        AtomicLong k = new AtomicLong(1L);
        new Thread(() -> {
            if (kafka.kafkaSend(jListServers.getSelectedItem() + ":" + jListPorts.getSelectedItem(),
                    Objects.requireNonNull(jListTopicsTo.getSelectedItem()).toString(),
                    String.valueOf(k.getAndIncrement()),
                    jTextArea1.getText(), jToggleButtonSsl.isSelected()).isDone()) {
                jTextArea2.setText("message has been successfully sent");
            } else {
                jTextArea2.setText("ERROR");
            }
        }).start();

    }

    private void jGetActionPerformed() {
        new Thread(() -> {
            Map<Long, String> resp = kafka.runConsumer(jListServers.getSelectedItem() + ":" + jListPorts.getSelectedItem(),
                    jGroupId.getText(), Objects.requireNonNull(jListTopicsFrom.getSelectedItem()).toString(), jToggleButtonSsl.isSelected());
            jTextArea2.setText("");
            resp.values().forEach(r ->  jTextArea2.append(r +"\n"));
        }).start();
    }

    private void jListTopicsFromActionPerformed() {
        // TODO add your handling code here:
    }

    private void jClearActionPerformed() {
        new Thread(() -> {
            jTextArea1.setText("");
            jTextArea2.setText("");
        }).start();
    }

    private void jListServersActionPerformed() {
        new Thread(() -> jListServers.getEditor().getEditorComponent().addKeyListener(new KeyAdapter() {
            @SneakyThrows
            public void keyPressed(KeyEvent evt1) {
                if (evt1.getKeyCode() == KeyEvent.VK_ENTER) {
                    saveUrls(jListServers.getSelectedItem() + ":" + jListPorts.getSelectedItem());

                    removeAllItems();
                }
            }
        })).start();
    }

    private void removeAllItems() {
        jListTopicsTo.removeAllItems();
        jListTopicsFrom.removeAllItems();

        kafka.getTopics(jListServers.getSelectedItem() + ":" + jListPorts.getSelectedItem(),
                jToggleButtonSsl.isSelected()).forEach(s -> jListTopicsTo.addItem(s));
        kafka.getTopics(jListServers.getSelectedItem() + ":" + jListPorts.getSelectedItem(),
                jToggleButtonSsl.isSelected()).forEach(s -> jListTopicsFrom.addItem(s));
    }

    private void refreshGetTopicsByServerNameActionPerformed() {
        new Thread(() -> {
            if (jListServers.getSelectedItem() != null && jListPorts.getSelectedItem() != null) {

                try {
                    saveUrls(jListServers.getSelectedItem() + ":" + jListPorts.getSelectedItem());
                } catch (IOException e) {
                    e.printStackTrace();
                }

                saveOrUpdate();
                removeAllItems();
            }
        }).start();

    }

    public void saveUrls(String data) throws IOException {
        String pathname = BOOTSTRAP_SERVERS_TXT;
        new File(pathname);
        String s = System.lineSeparator() + data;
        Files.write(Paths.get(pathname), s.getBytes(), StandardOpenOption.APPEND);
    }

    @PostConstruct
    public void onInit() {
        try {
            for (UIManager.LookAndFeelInfo info : UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(JFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        java.awt.EventQueue.invokeLater(() -> {
            setDefaultLookAndFeelDecorated(true);
            this.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
            this.setTitle("Tool for sending messages to Kafka");
            this.setCursor(new java.awt.Cursor(java.awt.Cursor.DEFAULT_CURSOR));
            this.setEnabled(true);
            this.setMinimumSize(new java.awt.Dimension(1133, 910));
            this.setPreferredSize(new java.awt.Dimension(1133, 910));
            this.setSize(new java.awt.Dimension(1133, 910));
            this.setLocationRelativeTo(null);
            this.setVisible(true);
        });
    }

    private JTextField jGroupId;
    private JComboBox<String> jListPorts;
    private JComboBox<String> jListServers;
    private JComboBox<String> jListTopicsFrom;
    private JComboBox<String> jListTopicsTo;
    private JTextArea jTextArea1;
    private JTextArea jTextArea2;
    private JToggleButton jToggleButtonSsl;
}
