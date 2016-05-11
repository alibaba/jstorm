/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.ui.tags;

import com.alibaba.jstorm.ui.model.ErrorEntity;
import com.alibaba.jstorm.ui.utils.UIUtils;
import com.google.common.base.Joiner;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.JspWriter;
import javax.servlet.jsp.tagext.SimpleTagSupport;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Jark (wuchong.wc@alibaba-inc.com)
 */
public class ErrorTag extends SimpleTagSupport {

    private static String[] WARNING_LIST = new String[]{"is full", "Backpressure", "backpressure"};

    private List<ErrorEntity> e;

    private boolean isWarning = false;

    public void setE(List<ErrorEntity> e) {
        this.e = e;
    }

    @Override
    public void doTag() throws JspException {
        JspWriter out = getJspContext().getOut();
        try {
            StringBuilder sb = new StringBuilder();
            if (e == null || e.size() == 0) {
//                sb.append("<span class='glyphicon glyphicon-ok-sign ok-msg'></span>");
//                sb.append("<span class='ok-msg'>N</span>");
            } else {
                String gly_cls = "glyphicon-remove-sign";
                String a_cls = "error-msg";
                String err_mgs = getErrorMsg();
                if (isWarning) {
                    gly_cls = "glyphicon-exclamation-sign";
                    a_cls = "warning-msg";
                }
                String err_content = getErrorContent();
                sb.append(String.format("<span tabindex='0' class='tip-msg pop %s'>%s</span>", a_cls, err_mgs));
                sb.append(String.format("<div class='hidden pop-content'>%s</div>", err_content));
            }
            out.write(sb.toString());
        } catch (IOException e) {
            throw new JspException("Error: " + e.getMessage());
        }
    }

    private String getErrorMsg() {
        for (ErrorEntity er : e) {
            if (isWarningMsg(er.getError())) {
                isWarning = true;
            }
        }
        int err_num = e.size();
        if (isWarning) {
            return String.format("W(%d)", err_num);
        } else {
            return String.format("E(%d)", err_num);
        }
    }

    private boolean isWarningMsg(String error){
        for (String s : WARNING_LIST){
            if (error.contains(s)){
                return true;
            }
        }
        return false;
    }

    private String getErrorContent() {
        List<String> ret = new ArrayList<>();
        for (ErrorEntity er : e) {
            long ts = ((long) er.getErrorTime()) * 1000;
            int index = er.getError().lastIndexOf(",");
            if (index == -1) {
                int idx = er.getError().indexOf("\n");
                int length = er.getError().length();
                if (idx != -1) {
                    String first_line = er.getError().substring(0, idx);
                    String rest_lines = er.getError().substring(idx + 1, length - 2);
                    ret.add(first_line + " , at " + UIUtils.parseDateTime(ts));
                    ret.add(rest_lines);
                }else{
                    ret.add(er.getError() + " , at " + UIUtils.parseDateTime(ts));
                }
            } else {
                ret.add(er.getError() + ", at " + UIUtils.parseDateTime(ts));
            }
        }
        Joiner joiner = Joiner.on("\n");
        return "<pre>" + joiner.join(ret) + "</pre>";
    }


}
